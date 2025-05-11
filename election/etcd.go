package election

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/conv"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// EtcdConfig 节点管理器配置器
type EtcdConfig struct {
	Endpoints []string
	Node      *Node
	Namespace string // 节点信息存储前缀
	LeaderKey string // 主节点选举前缀
}

// EtcdManager 节点管理器
type EtcdManager struct {
	client    *clientv3.Client
	session   *concurrency.Session
	election  *concurrency.Election
	node      Node
	namespace string // 节点信息存储前缀
	leaderKey string // 主节点选举前缀
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewEtcdNodeManager 新建节点管理器
func NewEtcdNodeManager(ctx context.Context, cfg *EtcdConfig) (*EtcdManager, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("请提供etcd地址")
	}

	childCtx, cancel := context.WithCancel(ctx)

	// 连接etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("连接etcd失败: %v", err)
	}

	// 创建会话
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		_ = cli.Close()
		cancel()
		return nil, fmt.Errorf("创建会话失败: %v", err)
	}
	node := NewNode(cfg.Node)

	if cfg.Namespace == "" {
		cancel()
		return nil, fmt.Errorf("namespace不能为空")
	}

	if cfg.LeaderKey == "" {
		cfg.LeaderKey = "leader"
	}

	return &EtcdManager{
		client:    cli,
		session:   session,
		election:  concurrency.NewElection(session, cfg.LeaderKey),
		node:      *node,
		namespace: cfg.Namespace,
		leaderKey: cfg.LeaderKey,
		ctx:       childCtx,
		cancel:    cancel,
	}, nil
}

// Start 启动节点：注册节点信息、参与选举、定时心跳
func (nm *EtcdManager) Start(initialLeaderID string, f func(node *Node) error) error {
	// 1. 注册节点信息（临时节点，会话结束自动删除）
	if err := nm.Register(); err != nil {
		return err
	}

	// 2. 定时发送心跳，更新最后活动时间
	go nm.heartbeat()

	// 3. 监听节点列表变化
	go nm.watchNodes()

	// 4. 参与主节点选举（如果是初始主节点则优先竞选）
	go func() {
		err := nm.participateElection(initialLeaderID, f)
		log.Println("participateElection:", err.Error())
	}()

	log.Printf("节点 %s 启动成功 (IP: %s)", nm.node.Id, nm.node.IP)
	return nil
}

func (nm *EtcdManager) getNodeKey() string {
	return nm.namespace + "/" + nm.node.Id
}

// 定时发送心跳
func (nm *EtcdManager) heartbeat() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			err := nm.Register()
			if err != nil {
				log.Printf("节点自动注册失败: %s (node: %s)", err.Error(), conv.String(nm.node))
			}
		}
	}
}

// 监听节点列表变化
func (nm *EtcdManager) watchNodes() {
	rch := nm.client.Watch(nm.ctx, nm.namespace, clientv3.WithPrefix())

	for wResp := range rch {
		for _, ev := range wResp.Events {
			node := new(Node)
			if err := conv.Unmarshal(string(ev.Kv.Value), node); err == nil {
				switch ev.Type {
				case clientv3.EventTypePut:
					log.Printf("节点更新: %s", string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					log.Printf("节点离线: %s", string(ev.Kv.Value))
				}
			}

			// 打印当前所有节点列表
			nm.printNodeList()
		}
	}
}

// 打印当前所有节点列表
func (nm *EtcdManager) printNodeList() {
	resp, err := nm.client.Get(nm.ctx, nm.namespace, clientv3.WithPrefix())
	if err != nil {
		log.Printf("获取节点列表失败: %v", err)
		return
	}

	log.Println("当前节点列表:")
	for _, kv := range resp.Kvs {
		var node Node
		if err = conv.Unmarshal(kv.Value, &node); err == nil {
			leaderMark := ""
			if node.IsLeader {
				leaderMark = " [主节点]"
			}
			log.Printf("- %s (IP: %s)%s", node.Id, node.IP, leaderMark)
		}
	}
}

// 参与主节点选举
func (nm *EtcdManager) participateElection(initialLeaderID string, f func(node *Node) error) error {
	// 如果是初始主节点，优先竞选
	if initialLeaderID == nm.node.Id {
		log.Println("作为初始主节点，尝试竞选...")
		err := nm.campaign(f)
		return err
	}

	// 否则先观察主节点，主节点失效后再竞选
	ch := nm.election.Observe(nm.ctx)
	for {
		select {
		case <-nm.ctx.Done():
			return nm.ctx.Err()
		case resp := <-ch:
			if len(resp.Kvs) == 0 {
				// 主节点可能已失效，尝试竞选
				log.Println("主节点可能已失效，尝试竞选...")
				return nm.campaign(f)
			}

			currentLeader := string(resp.Kvs[0].Value)
			log.Printf("当前主节点: %s", currentLeader)

			// 更新本地节点的主节点标识
			nm.node.IsLeader = currentLeader == nm.node.Id
		}
	}
}

// Register 注册节点信息到etcd
func (nm *EtcdManager) Register() error {
	nm.node.LastHeartbeat = time.Now()
	nodeData := conv.String(nm.node)
	// 创建临时节点（会话失效时自动删除）
	_, err := nm.client.Put(nm.ctx, nm.getNodeKey(), nodeData, clientv3.WithLease(nm.session.Lease()))
	return err
}

// LeaderRun 主节点运行
func (nm *EtcdManager) LeaderRun(f func(node *Node) error) (bool, error) {
	if nm.node.IsLeader {
		return true, f(&nm.node)
	}
	return false, nil
}

// 竞选主节点
func (nm *EtcdManager) campaign(f func(node *Node) error) error {
	if err := nm.election.Campaign(nm.ctx, nm.node.Id); err != nil {
		log.Printf("竞选主节点失败: %v", err)
		return err
	}

	// 成功成为主节点
	nm.node.IsLeader = true
	log.Printf("节点 %s 成为主节点，开始执行任务", nm.node.Id)

	// 更新节点信息中的主节点标识
	_ = nm.Register()

	// 主节点运行任务
	_, err := nm.LeaderRun(f)
	return err
}

// Stop 停止节点
func (nm *EtcdManager) Stop() {
	nm.cancel()
	_ = nm.session.Close()
	_ = nm.client.Close()

	// 如果是主节点，主动放弃
	if nm.node.IsLeader {
		_ = nm.election.Resign(nm.ctx)
	}

	log.Printf("节点 %s 已停止", nm.node.Id)
}
