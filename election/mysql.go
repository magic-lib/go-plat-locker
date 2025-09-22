package election

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/magic-lib/go-plat-locker/internal/mysqllock"
	"github.com/magic-lib/go-plat-locker/lock"
	"github.com/magic-lib/go-plat-utils/conv"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	lockerDefault = "_locker"
	stopDefault   = "_stop"
)

func init() { rand.Seed(time.Now().UnixNano()) }

var (
	electionMap = cmap.New[[]*Node]()
)

// MysqlElectionConfig 用于配置基于 MySQL 的选举相关参数
type MysqlElectionConfig struct {
	// 原有字段保持不变
	CacheConfig      cache.MySQLCacheConfig           `json:"cache_config" yaml:"cache_config"`           //选举结果存储位置
	LockerConfig     *mysqllock.Config                `json:"locker_config" yaml:"locker_config"`         //全局锁配置，默认使用mysql锁
	Locker           func(ns, key string) lock.Locker `json:"-" yaml:"-"`                                 //获取当前具体的锁，可能会与key相关，非全局，提高执行效率
	CurrentNode      *Node                            `json:"current_node" yaml:"current_node"`           //当前节点信息
	ElectionKey      string                           `json:"election_key" yaml:"election_key"`           // 选举所有List的Key，用于在CacheConfig存储的key
	HeartbeatTimeout time.Duration                    `json:"heartbeat_timeout" yaml:"heartbeat_timeout"` // 心跳超时时间，超时，表示这个节点断开了(秒)
	ElectionInterval time.Duration                    `json:"election_interval" yaml:"election_interval"` // 参与选举的周期，间隔多久重新选举，避免Leader节点挂掉的情况
	DefaultLeader    *Node                            `json:"default_leader" yaml:"default_leader"`       // 设置默认主节点，如果存在就默认为主节点

	// 新增首次选举控制参数
	FirstElectionTotalNodes int           `json:"first_election_total_nodes" yaml:"first_election_total_nodes"` // 预期参与首次选举的总节点数，0表示不限制
	FirstElectionMaxWait    time.Duration `json:"first_election_max_wait" yaml:"first_election_max_wait"`       // 首次选举的最大等待时间，超时后无论节点是否到齐都开始选举
}

// MysqlElection 选举管理器
type MysqlElection struct {
	mysqlCache     cache.CommCache[[]*Node]
	electionConfig MysqlElectionConfig
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	running        bool
}

// NewMysqlElection 创建选举实例
func NewMysqlElection(ctx context.Context, cfg *MysqlElectionConfig) (*MysqlElection, error) {
	if cfg.ElectionKey == "" {
		return nil, fmt.Errorf("选举Key不能为空，用来存储所有节点的列表")
	}
	childCtx, cancel := context.WithCancel(ctx)

	mysqlCache, err := cache.NewMySQLCache[[]*Node](&cfg.CacheConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建Mysql数据失败: %s", err.Error())
	}
	cfg.CurrentNode = NewNode(cfg.CurrentNode)

	if cfg.LockerConfig == nil {
		cfg.LockerConfig = &mysqllock.Config{
			DSN:       cfg.CacheConfig.DSN,
			TableName: cfg.CacheConfig.TableName + lockerDefault,
			Namespace: cfg.CacheConfig.Namespace + lockerDefault,
		}
	}

	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 300 * time.Second
	}
	if cfg.FirstElectionMaxWait == 0 {
		if cfg.FirstElectionTotalNodes <= 0 {
			cfg.FirstElectionMaxWait = 3 * time.Minute
		}
	}

	if cfg.LockerConfig.DSN == "" {
		cfg.LockerConfig.DSN = cfg.CacheConfig.DSN
	}
	if cfg.LockerConfig.Namespace == "" {
		cfg.LockerConfig.Namespace = cfg.CacheConfig.Namespace + lockerDefault
	}
	if cfg.LockerConfig.TableName == "" {
		cfg.LockerConfig.TableName = cfg.CacheConfig.TableName + lockerDefault
	}
	if cfg.Locker == nil {
		locker := lock.NewLocker(fmt.Sprintf("%s/%s", cfg.LockerConfig.Namespace, cfg.ElectionKey), lock.WithMysqlClient(cfg.LockerConfig))
		cfg.Locker = func(ns, key string) lock.Locker {
			return locker
		}
	}

	return &MysqlElection{
		electionConfig: *cfg,
		mysqlCache:     mysqlCache,
		ctx:            childCtx,
		cancel:         cancel,
		running:        true,
	}, nil
}

// Start 启动节点：注册节点信息、参与选举、定时心跳
func (nm *MysqlElection) Start(f func(node *Node) error) error {
	// 1. 注册节点信息
	if err := nm.register(); err != nil {
		return err
	}

	// 2. 定时发送心跳，更新最后活动时间,10s一次
	go nm.heartbeat()

	// 3. 参与主节点选举
	go func() {
		//等待第一次选举
		nm.waitForFirstElection()
		nm.startElection(f)
	}()

	log.Printf("节点 %s 启动成功 (IP: %s)", nm.electionConfig.CurrentNode.Id, nm.electionConfig.CurrentNode.IP)
	return nil
}

// Register 注册节点信息
func (nm *MysqlElection) register() error {
	nm.electionConfig.CurrentNode.LastHeartbeat = time.Now()
	return nm.setToList(nil)
}
func (nm *MysqlElection) delete(toNode *Node) error {
	return nm.setToList(func(nodeList []*Node) []*Node {
		newNodeList := make([]*Node, 0)
		lo.ForEach(nodeList, func(node *Node, index int) {
			if node.Compare(toNode) {
				return
			}
			newNodeList = append(newNodeList, node)
		})
		return newNodeList
	})
}

func (nm *MysqlElection) getElectionMapKey() string {
	return fmt.Sprintf("%s/%s", nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey)
}
func (nm *MysqlElection) getAllNodeFromCache() ([]*Node, error) {
	ctx := context.Background()
	var err error

	var nodeList []*Node
	mapKey := nm.getElectionMapKey()
	if electionMap.Has(mapKey) {
		nodeList, _ = electionMap.Get(mapKey)
	} else {
		nodeList, err = cache.NsGet[[]*Node](ctx, nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey)
		if err != nil {
			return nil, fmt.Errorf("获取节点列表失败: %v", err)
		}
	}
	return nodeList, nil
}

func (nm *MysqlElection) setListToCache(execList ...func(nodeList []*Node) []*Node) error {
	nodeList, err := nm.getAllNodeFromCache()
	if err != nil {
		return fmt.Errorf("获取节点列表失败: %v", err)
	}
	if nodeList == nil {
		nodeList = make([]*Node, 0)
	}

	log.Println("执行保存到mysql缓存:", nm.electionConfig.CurrentNode.Id, conv.String(nodeList))

	if len(execList) > 0 {
		lo.ForEach(execList, func(execFun func(nodeList []*Node) []*Node, index int) {
			if execFun == nil {
				return
			}
			nodeList = execFun(nodeList)
		})
	}

	sort.Slice(nodeList, func(i, j int) bool {
		one := nodeList[i]
		next := nodeList[j]
		if one.Priority > next.Priority {
			return true
		}
		if one.Priority == next.Priority {
			if one.LastHeartbeat.After(next.LastHeartbeat) {
				return true
			}
		}
		return false
	})

	mapKey := nm.getElectionMapKey()
	electionMap.Set(mapKey, nodeList)
	_, err = cache.NsSet[[]*Node](context.Background(), nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey, nodeList, 24*7*time.Hour)
	if err != nil {
		return fmt.Errorf("设置节点列表失败: %v", err)
	}
	return nil
}

func (nm *MysqlElection) setToList(execList func(nodeList []*Node) []*Node) error {
	var retErr error
	ctx := context.Background()
	err := nm.electionConfig.Locker(nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey).LockFunc(ctx, func() {
		retErr = nm.setListToCache(func(nodeList []*Node) []*Node {
			_, index, found := lo.FindIndexOf(nodeList, func(node *Node) bool {
				return node.Compare(nm.electionConfig.CurrentNode)
			})
			if !found {
				isStop, err := nm.isGlobalStop()
				if !(err == nil && isStop) {
					nodeList = append(nodeList, nm.electionConfig.CurrentNode)
				}
			} else {
				nodeList[index].LastHeartbeat = nm.electionConfig.CurrentNode.LastHeartbeat
			}
			return nodeList
		}, execList)
	})
	if err != nil {
		return fmt.Errorf("获取锁失败: %v", err)
	}
	if retErr != nil {
		return retErr
	}
	return nil
}

// startHeartbeat 启动心跳
func (nm *MysqlElection) heartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			err := nm.register()
			if err != nil {
				log.Printf("节点自动注册失败: %s (node: %s)", err.Error(), nm.electionConfig.CurrentNode.Id)
			}
		}
	}
}

// waitForFirstElection 等待第一次选举
func (nm *MysqlElection) waitForFirstElection() {
	//增加一个随机秒数，减少节点同时启动选举的几率
	randomDelay := time.Duration(rand.Intn(7)) * time.Second
	time.Sleep(randomDelay)

	// 直接等待指定超时时间后开始选举
	if nm.electionConfig.FirstElectionTotalNodes <= 0 {
		log.Printf(
			"首次选举：不限制节点数，等待 %v 后开始 (当前节点: %s)",
			nm.electionConfig.FirstElectionMaxWait,
			nm.electionConfig.CurrentNode.Id,
		)
		time.Sleep(nm.electionConfig.FirstElectionMaxWait)
		log.Printf("首次选举：等待超时，开始选举 (当前节点: %s)", nm.electionConfig.CurrentNode.Id)
		return
	}

	startTime := time.Now()
	targetNodes := nm.electionConfig.FirstElectionTotalNodes
	maxWait := nm.electionConfig.FirstElectionMaxWait
	lastErrorTime := time.Time{} // 记录上次错误发生时间，避免频繁打印错误日志

	log.Printf(
		"首次选举：等待 %d 个节点，最长等待 %v (当前节点: %s)",
		targetNodes, maxWait, nm.electionConfig.CurrentNode.Id,
	)

	for {
		// 检查是否超时（优先判断超时，避免错误时无限等待）
		if maxWait > 0 {
			elapsed := time.Since(startTime)
			if elapsed >= maxWait {
				log.Printf(
					"首次选举：等待超时 (%v/%v)，开始选举 (当前节点: %s)",
					elapsed, maxWait, nm.electionConfig.CurrentNode.Id,
				)
				return
			}
		}

		// 获取当前已注册的节点数量
		currList, err := nm.getAllNodeFromCache()
		if err != nil {
			// 控制错误日志打印频率，避免刷屏（10秒内只打印一次）
			if time.Since(lastErrorTime) > 10*time.Second {
				log.Printf(
					"首次选举：获取节点列表失败: %v (当前节点: %s，已等待: %v)",
					err, nm.electionConfig.CurrentNode.Id, time.Since(startTime),
				)
				lastErrorTime = time.Now()
			}
			time.Sleep(3 * time.Second)
			continue // 继续等待，直到超时
		}

		// 检查是否满足节点数量条件
		if len(currList) >= targetNodes {
			log.Printf(
				"首次选举：节点数量达标 (%d/%d)，开始选举 (当前节点: %s，已等待: %v)",
				len(currList), targetNodes, nm.electionConfig.CurrentNode.Id, time.Since(startTime),
			)
			return
		}

		// 未满足条件，3秒后再次检查
		time.Sleep(3 * time.Second)
	}
}

// StartElection 开始选举过程
func (nm *MysqlElection) startElection(fun func(node *Node) error) {
	currentNodeID := nm.electionConfig.CurrentNode.Id // 提取当前节点ID，简化日志
	interval := nm.electionConfig.ElectionInterval

	defer func() {
		//清理资源
		if err := nm.Stop(); err != nil {
			log.Printf("选举停止失败 (节点: %s): %v", currentNodeID, err)
		} else {
			log.Printf("选举完成并停止 (节点: %s)", currentNodeID)
		}
	}()

	// 一次性选举模式
	if interval == 0 {
		log.Printf("启动一次性选举 (节点: %s)", currentNodeID)
		if err := nm.electOnce(fun); err != nil {
			log.Printf("一次性选举失败 (节点: %s): %v", currentNodeID, err)
		}
		return
	}

	// 周期性选举模式
	ticker := time.NewTicker(interval) // 明确单位为秒
	defer ticker.Stop()

	log.Printf("启动周期性选举，间隔 %ds (节点: %s)", interval, currentNodeID)

	for {
		select {
		case <-nm.ctx.Done():
			log.Printf("startElection ctx.Done: %s", conv.String(nm.electionConfig.CurrentNode))
			return
		case <-ticker.C:
			// 检查是否全局停止
			isStop, err := nm.isGlobalStop()
			if err != nil {
				log.Printf("检查全局停止状态失败 (节点: %s): %v", currentNodeID, err)
				continue // 错误时继续下一次选举
			}

			if isStop {
				log.Printf("检测到全局停止信号，终止选举 (节点: %s)", currentNodeID)
				return
			}

			// 执行单次选举
			if err := nm.electOnce(fun); err != nil {
				log.Printf("周期性选举失败 (节点: %s): %v", currentNodeID, err)
			}
		}
	}
}
func (nm *MysqlElection) removeExpireNode(nodeList []*Node) []*Node {
	newNodeList := make([]*Node, 0)
	// 去掉超时的节点
	lo.ForEach(nodeList, func(node *Node, index int) {
		if node.LastHeartbeat.Add(nm.electionConfig.HeartbeatTimeout).Before(time.Now()) {
			return
		}
		newNodeList = append(newNodeList, node)
	})
	return newNodeList
}
func (nm *MysqlElection) findLeader(nodeList []*Node) (*Node, int) {
	if len(nodeList) == 0 {
		return nil, 0
	}
	if nm.electionConfig.DefaultLeader != nil {
		oneNode, index, ok := lo.FindIndexOf(nodeList, func(node *Node) bool {
			return nm.electionConfig.DefaultLeader.CheckIsLeader(node)
		})
		if ok {
			return oneNode, index
		}
	}

	oneNode, index, ok := lo.FindIndexOf(nodeList, func(node *Node) bool {
		return node.IsLeader
	})
	if ok {
		//fmt.Println("选择当前Leader主节点:", oneNode.Id)
		return oneNode, index
	}
	return nodeList[0], 0
}

// electOnce 执行一次选举
func (nm *MysqlElection) electOnce(fun func(node *Node) error) error {
	return nm.setToList(func(nodeList []*Node) []*Node {
		//echoId := make([]string, 0)
		//lo.ForEach(nodeList, func(item *Node, index int) {
		//	echoId = append(echoId, item.Id, conv.String(item.IsLeader), conv.String(int64(time.Now().Sub(item.LastHeartbeat).Seconds())))
		//})
		//fmt.Println("allList:", echoId)

		nodeList = nm.removeExpireNode(nodeList)
		leaderNode, index := nm.findLeader(nodeList)

		if leaderNode != nil {
			log.Printf("当前Leader节点: %s (node: %s)", leaderNode.Id, conv.String(leaderNode))

			lo.ForEach(nodeList, func(node *Node, index int) {
				node.IsLeader = false //其他的都变为非主节点
			})
			nodeList[index].IsLeader = true
			if nm.electionConfig.CurrentNode.Compare(leaderNode) && nm.running {
				err := fun(leaderNode)
				if err != nil {
					log.Println("主节点任务执行失败:", err)
				}
			}
		}
		return nodeList
	})
}

// setGlobalStop 存储停止功能
func (nm *MysqlElection) setGlobalStop() error {
	nm.running = false
	ctx := context.Background()
	_, err := cache.NsSet[[]*Node](ctx, nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey+stopDefault, nil, 24*time.Hour)
	if err != nil {
		return fmt.Errorf("设置节点停止失败: %v", err)
	}
	return nil
}
func (nm *MysqlElection) clearGlobalStop() error {
	nodeList, err := nm.getAllNodeFromCache()
	if err != nil {
		return fmt.Errorf("获取节点列表失败clearGlobalStop: %v", err)
	}
	nodeList = nm.removeExpireNode(nodeList)
	if len(nodeList) == 0 {
		ctx := context.Background()
		_, err = cache.NsDel[[]*Node](ctx, nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey+stopDefault)
		if err != nil {
			return fmt.Errorf("设置节点停止失败clearGlobalStop: %v", err)
		}
		_, _ = cache.NsDel[[]*Node](ctx, nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey)
		return nil
	}
	return nil
}

func (nm *MysqlElection) isGlobalStop() (bool, error) {
	if !nm.running {
		return true, nil
	}

	ctx := context.Background()
	data, err := cache.NsGet[[]*Node](ctx, nm.mysqlCache, nm.electionConfig.CacheConfig.Namespace, nm.electionConfig.ElectionKey+stopDefault)
	if err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, nil
	}
	nm.running = false
	return true, nil
}

// Stop 停止选举
func (nm *MysqlElection) Stop() error {
	err := nm.setGlobalStop()
	if err != nil {
		return err
	}
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.cancel()

	nodeId := nm.electionConfig.CurrentNode.Id

	err = nm.delete(nm.electionConfig.CurrentNode)
	if err != nil {
		return err
	}
	//如果全部删除了，则执行删除Stop
	err = nm.clearGlobalStop()
	if err != nil {
		return err
	}

	log.Printf("节点 %s 停止选举\n", conv.String(nodeId))
	return nil
}
