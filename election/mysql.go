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
	"sort"
	"sync"
	"time"
)

const (
	lockerDefault = "_locker"
	stopDefault   = "_stop"
)

var (
	electionMap = cmap.New[[]*Node]()
)

// MysqlConfig 节点管理器配置器
type MysqlConfig struct {
	CacheConfig      cache.MySQLCacheConfig
	LockerConfig     *mysqllock.Config
	Locker           func(ns, key string) lock.Locker
	Node             *Node
	ElectionKey      string // 选举List的所有Key
	HeartbeatTimeout int    // 心跳超时时间，超时，表示这个节点不能用了(秒)
	ElectionInterval int    // 选举检查间隔(秒)
	DefaultLeader    *Node  // 设置默认主节点，如果存在就默认为主节点
}

// MysqlElection 选举管理器
type MysqlElection struct {
	mysqlCache    cache.CommCache[[]*Node]
	lockFunc      func(ns, key string) lock.Locker
	node          *Node
	defaultLeader *Node
	mysqlConfig   *MysqlConfig
	mu            sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	running       bool
}

// NewMysqlElection 创建选举实例
func NewMysqlElection(ctx context.Context, cfg *MysqlConfig) (*MysqlElection, error) {
	if cfg.ElectionKey == "" {
		return nil, fmt.Errorf("选举Key不能为空，用来存储所有节点的列表")
	}
	childCtx, cancel := context.WithCancel(ctx)

	mysqlCache, err := cache.NewMySQLCache[[]*Node](&cfg.CacheConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建Mysql数据失败: %s", err.Error())
	}
	node := NewNode(cfg.Node)

	if cfg.LockerConfig == nil {
		cfg.LockerConfig = &mysqllock.Config{
			DSN:       cfg.CacheConfig.DSN,
			TableName: cfg.CacheConfig.TableName + lockerDefault,
			Namespace: cfg.CacheConfig.Namespace + lockerDefault,
		}
	}
	if cfg.ElectionInterval == 0 {
		cfg.ElectionInterval = 150
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 300
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
		mysqlConfig:   cfg,
		mysqlCache:    mysqlCache,
		lockFunc:      cfg.Locker,
		node:          node,
		defaultLeader: cfg.DefaultLeader,
		ctx:           childCtx,
		cancel:        cancel,
		running:       true,
	}, nil
}

// Start 启动节点：注册节点信息、参与选举、定时心跳
func (nm *MysqlElection) Start(f func(node *Node) error) error {
	// 1. 注册节点信息
	if err := nm.Register(); err != nil {
		return err
	}

	// 2. 定时发送心跳，更新最后活动时间
	go nm.heartbeat()

	// 3. 参与主节点选举（如果是初始主节点则优先竞选）
	go func() {
		nm.startElection(f)
	}()

	log.Printf("节点 %s 启动成功 (IP: %s)", nm.node.Id, nm.node.IP)
	return nil
}

// Register 注册节点信息
func (nm *MysqlElection) Register() error {
	nm.node.LastHeartbeat = time.Now()
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
	return fmt.Sprintf("%s/%s", nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey)
}
func (nm *MysqlElection) getAllNodeFromCache() ([]*Node, error) {
	ctx := context.Background()
	var err error

	var nodeList []*Node
	mapKey := nm.getElectionMapKey()
	if electionMap.Has(mapKey) {
		nodeList, _ = electionMap.Get(mapKey)
	} else {
		nodeList, err = cache.NsGet[[]*Node](ctx, nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey)
		if err != nil {
			return nil, fmt.Errorf("获取节点列表失败: %v", err)
		}
	}
	return nodeList, nil
}

func (nm *MysqlElection) setListToCache(execList ...func(nodeList []*Node) []*Node) error {
	log.Println("执行保存到mysql缓存:", nm.node.Id)

	nodeList, err := nm.getAllNodeFromCache()
	if err != nil {
		return fmt.Errorf("获取节点列表失败: %v", err)
	}
	if nodeList == nil {
		nodeList = make([]*Node, 0)
	}

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
	_, err = cache.NsSet[[]*Node](context.Background(), nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey, nodeList, 24*7*time.Hour)
	if err != nil {
		return fmt.Errorf("设置节点列表失败: %v", err)
	}
	return nil
}

func (nm *MysqlElection) setToList(execList func(nodeList []*Node) []*Node) error {
	var retErr error
	ctx := context.Background()
	err := nm.lockFunc(nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey).LockFunc(ctx, func() {
		retErr = nm.setListToCache(func(nodeList []*Node) []*Node {
			_, index, found := lo.FindIndexOf(nodeList, func(node *Node) bool {
				return node.Compare(nm.node)
			})
			if !found {
				isStop, err := nm.isGlobalStop()
				if !(err == nil && isStop) {
					nodeList = append(nodeList, nm.node)
				}
			} else {
				nodeList[index].LastHeartbeat = nm.node.LastHeartbeat
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
			err := nm.Register()
			if err != nil {
				log.Printf("节点自动注册失败: %s (node: %s)", err.Error(), nm.node.Id)
			}
		}
	}
}

// StartElection 开始选举过程
func (nm *MysqlElection) startElection(fun func(node *Node) error) {
	ticker := time.NewTicker(time.Duration(nm.mysqlConfig.ElectionInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-nm.ctx.Done():
			log.Printf("startElection ctx.Done: %s", conv.String(nm.node))
			_ = nm.Stop()
			return
		case <-ticker.C:
			isStop, err := nm.isGlobalStop()
			if isStop && err == nil {
				log.Printf("startElection ticker.C: %s, %s", conv.String(nm.node), conv.String(isStop))
				_ = nm.Stop()
			} else {
				err = nm.electOnce(fun)
				if err != nil {
					log.Printf("节点选举失败: %s (node: %s)", err.Error(), conv.String(nm.node))
				}
			}
		}
	}
}
func (nm *MysqlElection) removeExpireNode(nodeList []*Node) []*Node {
	newNodeList := make([]*Node, 0)
	// 去掉超时的节点
	lo.ForEach(nodeList, func(node *Node, index int) {
		if node.LastHeartbeat.Add(time.Duration(nm.mysqlConfig.HeartbeatTimeout) * time.Second).Before(time.Now()) {
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
	if nm.defaultLeader != nil {
		oneNode, index, ok := lo.FindIndexOf(nodeList, func(node *Node) bool {
			return nm.defaultLeader.CheckIsLeader(node)
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
			if nm.node.Compare(leaderNode) && nm.running {
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
	_, err := cache.NsSet[[]*Node](ctx, nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey+stopDefault, nil, 24*time.Hour)
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
	if len(nodeList) == 0 {
		ctx := context.Background()
		_, err = cache.NsDel[[]*Node](ctx, nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey+stopDefault)
		if err != nil {
			return fmt.Errorf("设置节点停止失败clearGlobalStop: %v", err)
		}
		_, _ = cache.NsDel[[]*Node](ctx, nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey)
	}
	return nil
}

func (nm *MysqlElection) isGlobalStop() (bool, error) {
	if !nm.running {
		return true, nil
	}

	ctx := context.Background()
	data, err := cache.NsGet[[]*Node](ctx, nm.mysqlCache, nm.mysqlConfig.CacheConfig.Namespace, nm.mysqlConfig.ElectionKey+stopDefault)
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

	nodeId := nm.node.Id

	err = nm.delete(nm.node)
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
