package election_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"github.com/magic-lib/go-plat-locker/election"
	"github.com/magic-lib/go-plat-utils/conv"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestEtcd(t *testing.T) {
	// 配置
	//etcdEndpoints := []string{"localhost:2379"} // etcd地址
	nodeID := os.Getenv("NODE_ID") // 从环境变量获取节点ID
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", time.Now().UnixNano()%1000) // 默认生成节点ID
	}
	initialLeaderID := "node-1" // 初始主节点ID

	// 创建节点管理器
	manager, err := election.NewEtcdNodeManager(context.Background(), nil)
	if err != nil {
		log.Fatalf("初始化节点管理器失败: %v", err)
	}
	defer manager.Stop()

	// 启动节点
	if err := manager.Start(initialLeaderID, func(node *election.Node) error {
		fmt.Printf("节点启动成功，当前节点ID: %s\n", node.Id)
		return nil
	}); err != nil {
		log.Fatalf("启动节点失败: %v", err)
	}

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("开始关闭节点...")
}
func TestMysql(t *testing.T) {

	dsn := "root:tianlin0@tcp(127.0.0.1:3306)/huji?charset=utf8mb4&parseTime=True&loc=Local"
	ns := "test"
	tableName := "mysql_cache"
	key := "leader_key"

	cacheCfg := cache.MySQLCacheConfig{
		DSN:       dsn,
		TableName: tableName,
		Namespace: ns,
	}

	go func() {
		ctx := context.Background()
		node1, err := election.NewMysqlElection(ctx, &election.MysqlElectionConfig{
			ElectionInterval: 0,
			HeartbeatTimeout: 0,
			CurrentNode: &election.Node{
				Id: "node1",
			},
			CacheConfig: cacheCfg,
			ElectionKey: key,
		})
		if err != nil {
			panic(err)
		}

		err = node1.Start(func(node *election.Node) error {
			fmt.Println("执行主节点对象：", conv.String(node))
			_ = node1.Stop()
			return nil
		})

		if err != nil {
			panic(err)
		}

		select {}
	}()
	go func() {
		ctx := context.Background()
		node1, err := election.NewMysqlElection(ctx, &election.MysqlElectionConfig{
			ElectionInterval: 0,
			HeartbeatTimeout: 0,
			CurrentNode: &election.Node{
				Id: "node2",
			},
			CacheConfig: cacheCfg,
			ElectionKey: key,
		})
		if err != nil {
			panic(err)
		}

		err = node1.Start(func(node *election.Node) error {
			fmt.Println("执行主节点对象：", conv.String(node))
			_ = node1.Stop()
			return nil
		})

		if err != nil {
			panic(err)
		}

		select {}
	}()
	go func() {
		ctx := context.Background()
		node1, err := election.NewMysqlElection(ctx, &election.MysqlElectionConfig{
			ElectionInterval: 0,
			HeartbeatTimeout: 0,
			CurrentNode: &election.Node{
				Id: "node3",
			},
			CacheConfig: cacheCfg,
			ElectionKey: key,
		})
		if err != nil {
			panic(err)
		}

		err = node1.Start(func(node *election.Node) error {
			fmt.Println("执行主节点对象：", conv.String(node))
			_ = node1.Stop()
			return nil
		})

		if err != nil {
			panic(err)
		}

		select {}
	}()

	select {}
}
