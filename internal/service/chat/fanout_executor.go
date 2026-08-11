package chat

import (
	"hash/fnv"
	"sync"

	"kama_chat_server/pkg/zlog"
)

const (
	// fanoutWorkerCount 扇出 worker 数：同一群哈希分片到固定 worker，串行保序
	fanoutWorkerCount = 8
	// fanoutQueueSize 每个 worker 的任务队列容量：满则丢弃（消息已落库，离线可从历史拉取）
	fanoutQueueSize = 1024
)

// fanoutTask 群消息扇出任务（写扩散模型：一条消息广播到群内所有在线成员）
type fanoutTask struct {
	groupId     string
	messageBack *MessageBack
}

// FanoutExecutor 群消息扇出协程池。
//
// 设计：写扩散（Write Amplification）——消息在会话 worker 里只构造一次 messageBack（
// 一条 JSON），由协程池广播给 N 个在线成员的投递队列，把扇出计算从会话 worker
// （在线聊天主线程）中解耦出来，线性循环变并行分片。
//
// 保序关键：按群哈希分片（shard），同一群的扇出任务永远进入同一 worker 的
// 同一 FIFO 队列，由该 worker 串行投递 → 群内消息到达顺序 = 入队顺序，
// 与改造前"会话 worker 同步扇出"的顺序语义一致。
type FanoutExecutor struct {
	workers   []chan *fanoutTask
	clients   map[string]*Client
	mutex     *sync.RWMutex
	quit      chan struct{}
	closeOnce sync.Once
}

// NewFanoutExecutor 创建扇出协程池并启动全部 worker
func NewFanoutExecutor(clients map[string]*Client, mutex *sync.RWMutex) *FanoutExecutor {
	fe := &FanoutExecutor{
		workers: make([]chan *fanoutTask, fanoutWorkerCount),
		clients: clients,
		mutex:   mutex,
		quit:    make(chan struct{}),
	}
	for i := 0; i < fanoutWorkerCount; i++ {
		fe.workers[i] = make(chan *fanoutTask, fanoutQueueSize)
		go fe.worker(i)
	}
	return fe
}

// Submit 提交扇出任务（非阻塞：队列满则丢弃并计数——消息已落库，
// 实时投递尽力而为，与 EnqueueDelivery 的 500ms 超时取舍一致）
func (fe *FanoutExecutor) Submit(groupId string, messageBack *MessageBack) {
	worker := fe.shard(groupId)
	select {
	case <-fe.quit:
		return
	case fe.workers[worker] <- &fanoutTask{groupId: groupId, messageBack: messageBack}:
	default:
		ChatMetrics.fanoutQueueDrops.Add(1)
		zlog.Warn("群扇出队列已满，丢弃扇出任务: " + groupId)
	}
}

// shard 群哈希分片：同一群固定同一 worker（保序关键）
func (fe *FanoutExecutor) shard(groupId string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(groupId))
	return int(h.Sum32()) % len(fe.workers)
}

func (fe *FanoutExecutor) worker(id int) {
	for {
		select {
		case <-fe.quit:
			return
		case task := <-fe.workers[id]:
			fe.fanout(task)
		}
	}
}

// fanout 执行写扩散：取成员（Redis Set 缓存，miss 才查 DB）→
// 锁内快照在线客户端引用 → 锁外逐个投递（防慢客户端卡住 map 锁）
func (fe *FanoutExecutor) fanout(task *fanoutTask) {
	members, err := getActiveGroupMemberIDs(task.groupId)
	if err != nil {
		zlog.Error("扇出获取群成员失败: " + err.Error())
		return
	}
	targets := make([]*Client, 0, len(members))
	fe.mutex.RLock()
	for _, member := range members {
		if client, ok := fe.clients[member]; ok {
			targets = append(targets, client)
		}
	}
	fe.mutex.RUnlock()
	for _, client := range targets {
		if err := client.EnqueueDelivery(task.messageBack); err != nil {
			zlog.Warn("群扇出投递失败: " + err.Error())
		}
	}
}

// Close 停止全部 worker（排空已入队任务由 worker 的 quit 分支丢弃剩余）
func (fe *FanoutExecutor) Close() {
	fe.closeOnce.Do(func() {
		close(fe.quit)
	})
}
