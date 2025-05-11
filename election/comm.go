package election

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/utils/httputil/param"
	"os"
	"time"
)

// Node 节点信息结构体
type Node struct {
	Id            string    `json:"id"`        // 节点唯一标识
	IP            string    `json:"ip"`        // 节点IP
	Hostname      string    `json:"hostname"`  // 节点主机名
	IsLeader      bool      `json:"is_leader"` // 是否是leader
	Priority      int       `json:"priority"`  // 优先级
	LastHeartbeat time.Time `json:"last_seen"` // 最后心跳时间
}

func NewNode(node *Node) *Node {
	if node == nil {
		node = new(Node)
	}
	if node.Hostname == "" {
		node.Hostname, _ = os.Hostname()
	}

	if node.IP == "" {
		ip, err := param.IPv4()
		if err == nil {
			node.IP = ip.String()
		}
	}
	if cond.IsTimeEmpty(node.LastHeartbeat) {
		node.LastHeartbeat = time.Now()
	}
	if node.Id == "" {
		node.Id = fmt.Sprintf("%s-%s", node.Hostname, node.IP)
	}
	return node
}

func (n *Node) Compare(to *Node) bool {
	if n.Id == to.Id && n.IP == to.IP && n.Hostname == to.Hostname {
		return true
	}
	return false
}

type LeaderElector interface {
	Register() error                                  // 注册节点
	LeaderRun(f func(node *Node) error) (bool, error) // leader执行任务
}
