// Command ws_load measures end-to-end WebSocket message delivery latency.
// It must be run only against an environment dedicated to testing.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"

	_ "github.com/go-sql-driver/mysql"
)

type user struct {
	UUID      string `json:"uuid"`
	Nickname  string `json:"nickname"`
	Token     string `json:"token"`
	Email     string `json:"email,omitempty"`
	Telephone string `json:"telephone,omitempty"`
	Password  string `json:"password,omitempty"`
}

// loginID 返回登录凭证（优先 email，回退 telephone）——当前系统 login 接口以 email 为主
func (u *user) loginID() string {
	if u.Email != "" {
		return u.Email
	}
	return u.Telephone
}

type message struct {
	SessionID  string `json:"session_id"`
	Type       int    `json:"type"`
	Content    string `json:"content"`
	URL        string `json:"url"`
	SendID     string `json:"send_id"`
	SendName   string `json:"send_name"`
	SendAvatar string `json:"send_avatar"`
	ReceiveID  string `json:"receive_id"`
	FileSize   string `json:"file_size"`
	FileType   string `json:"file_type"`
	FileName   string `json:"file_name"`
	AVData     string `json:"av_data"`
}

type result struct {
	RunID                    string    `json:"run_id"`
	StartedAt                time.Time `json:"started_at"`
	FinishedAt               time.Time `json:"finished_at"`
	Users                    int       `json:"users"`
	RatePerUser              int       `json:"rate_per_user"`
	MessagesRequested        int       `json:"messages_requested"`
	MessagesReceived         int       `json:"messages_received"`
	MessagesPersisted        int       `json:"messages_persisted"`
	PersistenceChecked       bool      `json:"persistence_checked"`
	KafkaLag                 int64     `json:"kafka_topic_end_offsets"`
	KafkaConsumerLag         int64     `json:"kafka_consumer_lag"`
	KafkaLagChecked          bool      `json:"kafka_lag_checked"`
	Duplicates               int       `json:"duplicates"`
	SequenceGaps             int       `json:"sequence_gaps"`
	ChannelRouted            uint64    `json:"channel_routed"`
	KafkaRouted              uint64    `json:"kafka_routed"`
	BatchFlushErrors         uint64    `json:"batch_flush_errors"`
	DeliveryQueueTimeouts    uint64    `json:"delivery_queue_timeouts"`
	DeliveryClientClosed     uint64    `json:"delivery_client_closed"`
	DeliveryStatusQueueDrops uint64    `json:"delivery_status_queue_drops"`
	SessionQueueTimeouts     uint64    `json:"session_queue_timeouts"`
	ProcessFailures          uint64    `json:"process_failures"`
	KafkaCommitFailures      uint64    `json:"kafka_commit_failures"`
	MetricsCollected         bool      `json:"metrics_collected"`
	ConnectionErrors         int64     `json:"connection_errors"`
	WriteErrors              int64     `json:"write_errors"`
	DurationMS               float64   `json:"duration_ms"`
	SendDurationMS           float64   `json:"send_duration_ms"`
	DeliveryDurationMS       float64   `json:"delivery_duration_ms"`
	Throughput               float64   `json:"throughput_msg_per_sec"`
	SendThroughput           float64   `json:"send_throughput_msg_per_sec"`
	Completed                bool      `json:"completed"`
	TimedOut                 bool      `json:"timed_out"`
	SuccessRate              float64   `json:"success_rate_percent"`
	P50MS                    float64   `json:"p50_ms"`
	P95MS                    float64   `json:"p95_ms"`
	P99MS                    float64   `json:"p99_ms"`
}

type client struct {
	user user
	conn *websocket.Conn
}

type pendingDelivery struct {
	startedAt    time.Time
	receiverUUID string
}

// seqTracker tracks received sequence numbers per sender UUID for gap/duplicate detection.
type seqTracker struct {
	mu   sync.Mutex
	data map[string]map[int]int // senderUUID -> seq -> receive count
}

func newSeqTracker() *seqTracker {
	return &seqTracker{data: make(map[string]map[int]int)}
}

func (st *seqTracker) record(senderUUID string, seq int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	m, ok := st.data[senderUUID]
	if !ok {
		m = make(map[int]int)
		st.data[senderUUID] = m
	}
	m[seq]++
}

// computeGaps returns (missing sequences, duplicate deliveries) given expected perUser count and sender list.
func (st *seqTracker) computeGaps(perUser int, senders []string) (gaps, duplicates int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, sender := range senders {
		m := st.data[sender]
		for seq := 0; seq < perUser; seq++ {
			if m[seq] == 0 {
				gaps++
			}
		}
		for _, count := range m {
			if count > 1 {
				duplicates += count - 1
			}
		}
	}
	return
}

// parsePerfContent extracts runID, sender UUID and sequence from "perf:runID:UUID:seq:ts".
func parsePerfContent(content string) (runID, senderUUID string, seq int, ok bool) {
	parts := strings.SplitN(content, ":", 5)
	if len(parts) != 5 || parts[0] != "perf" {
		return "", "", 0, false
	}
	seq, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, false
	}
	return parts[1], parts[2], seq, true
}

func main() {
	usersPath := flag.String("users", "", "JSON file containing at least two distinct test users")
	wsURL := flag.String("url", "wss://127.0.0.1:8000/wss", "WebSocket endpoint without the token query parameter")
	perUser := flag.Int("messages-per-user", 100, "text messages sent by every test user")
	ratePerUser := flag.Int("rate-per-user", 0, "per-user send rate; 0 sends a burst")
	timeout := flag.Duration("timeout", 30*time.Second, "maximum time to wait for delivery")
	persistenceTimeout := flag.Duration("persistence-timeout", 15*time.Second, "maximum time to wait for all message rows to persist")
	kafkaLagTimeout := flag.Duration("kafka-lag-timeout", 15*time.Second, "maximum time to wait for Kafka consumer lag to reach zero")
	out := flag.String("out", "", "optional JSON result file")
	dsn := flag.String("dsn", "", "MySQL DSN for DB row count verification (empty skips DB check)")
	kafkaBroker := flag.String("kafka", "", "Kafka broker address for lag check (empty skips Kafka check)")
	kafkaTopic := flag.String("kafka-topic", "chat_message", "Kafka topic name")
	kafkaGroup := flag.String("kafka-group", "chat", "Kafka consumer group name")
	hold := flag.Duration("hold", 0, "hold connections open for this duration after all messages are received (e.g. 5m for 500-connection stability test)")
	metricsURL := flag.String("metrics-url", "https://127.0.0.1:8000/metrics", "HTTP endpoint to query server-side route counters (empty skips)")
	flag.Parse()

	if (*usersPath == "" && os.Getenv("KAMA_PERF_USERS") == "") || *perUser < 1 {
		log.Fatal("-users (or KAMA_PERF_USERS) and a positive -messages-per-user are required")
	}
	users := authenticateUsers(loadUsers(*usersPath), *wsURL)
	if len(users) < 2 {
		log.Fatal("at least two users with distinct UUIDs and non-empty tokens are required")
	}

	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // Test environments use the local self-signed certificate.
	clients := make([]*client, 0, len(users))
	var connectionErrors int64
	for _, u := range users {
		endpoint := *wsURL + "?token=" + u.Token
		conn, _, err := dialer.Dial(endpoint, nil)
		if err != nil {
			atomic.AddInt64(&connectionErrors, 1)
			log.Printf("connect %s: %v", u.UUID, err)
			continue
		}
		clients = append(clients, &client{user: u, conn: conn})
	}
	defer func() {
		for _, c := range clients {
			_ = c.conn.Close()
		}
	}()
	if len(clients) < 2 {
		log.Fatalf("only %d connections succeeded", len(clients))
	}

	// Collect sender UUIDs for gap detection
	senderUUIDs := make([]string, len(clients))
	for i, c := range clients {
		senderUUIDs[i] = c.user.UUID
	}

	total := len(clients) * *perUser
	runID := fmt.Sprintf("%x", time.Now().UnixNano())
	// Route counters are process-wide. Capture a baseline so each result is
	// scoped to this run even when the server stays up between scenarios.
	var metricsBefore metricsSnapshot
	metricsAvailable := false
	if *metricsURL != "" {
		metricsBefore, metricsAvailable = queryMetrics(*metricsURL)
	}
	started := time.Now()
	pending := make(map[string]pendingDelivery, total)
	receiverLatencies := make([]time.Duration, 0, total)
	var pendingMu sync.Mutex
	var received int64
	var lastReceivedAt int64
	var writeErrors int64
	done := make(chan struct{})
	var doneOnce sync.Once
	tracker := newSeqTracker()

	for _, c := range clients {
		go readDeliveries(c.conn, c.user.UUID, &pendingMu, pending, &receiverLatencies, &received, &lastReceivedAt, total, done, &doneOnce, tracker)
	}

	var writers sync.WaitGroup
	for i, c := range clients {
		receiver := clients[(i+1)%len(clients)]
		writers.Add(1)
		go func(sender, receiver *client) {
			defer writers.Done()
			var ticker *time.Ticker
			if *ratePerUser > 0 {
				ticker = time.NewTicker(time.Second / time.Duration(*ratePerUser))
				defer ticker.Stop()
			}
			for sequence := 0; sequence < *perUser; sequence++ {
				if ticker != nil {
					<-ticker.C
				}
				content := fmt.Sprintf("perf:%s:%s:%d:%d", runID, sender.user.UUID, sequence, time.Now().UnixNano())
				pendingMu.Lock()
				pending[content] = pendingDelivery{startedAt: time.Now(), receiverUUID: receiver.user.UUID}
				pendingMu.Unlock()
				payload := message{
					SessionID: sender.user.UUID + "-" + receiver.user.UUID,
					Type:      0, Content: content, SendID: sender.user.UUID, SendName: sender.user.Nickname,
					SendAvatar: "/static/avatars/perf.png", ReceiveID: receiver.user.UUID, FileSize: "0B",
				}
				if err := sender.conn.WriteJSON(payload); err != nil {
					atomic.AddInt64(&writeErrors, 1)
				}
			}
		}(c, receiver)
	}
	writers.Wait()
	writesFinished := time.Now()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(*timeout):
	}

	finished := time.Now()

	// Hold connections open if requested (for connection stability testing)
	if *hold > 0 {
		log.Printf("All messages received, holding %d connections for %v...", len(clients), *hold)
		time.Sleep(*hold)
	}

	// Sequence gap / duplicate detection
	gaps, duplicates := tracker.computeGaps(*perUser, senderUUIDs)

	// DB row count verification
	msgPersisted := 0
	persistenceChecked := false
	if *dsn != "" {
		msgPersisted, persistenceChecked = waitForPersisted(*dsn, runID, started, total, *persistenceTimeout)
	}

	// Kafka lag check
	kafkaTopicEndOffsets := int64(0)
	kafkaConsumerLag := int64(0)
	kafkaLagChecked := false
	if *kafkaBroker != "" {
		kafkaTopicEndOffsets, kafkaConsumerLag, kafkaLagChecked = waitForKafkaLag(*kafkaBroker, *kafkaTopic, *kafkaGroup, *kafkaLagTimeout)
	}

	// Server-side route counters
	var metrics metricsSnapshot
	if metricsAvailable {
		var metricsAfterAvailable bool
		metrics, metricsAfterAvailable = queryMetrics(*metricsURL)
		metricsAvailable = metricsAfterAvailable
		if metricsAvailable {
			metrics = metrics.subtract(metricsBefore)
		}
	}

	r := buildResult(runID, started, writesFinished, finished, time.Unix(0, atomic.LoadInt64(&lastReceivedAt)),
		completed, len(clients), *ratePerUser, total, int(atomic.LoadInt64(&received)),
		connectionErrors, writeErrors, receiverLatencies, msgPersisted, kafkaTopicEndOffsets, kafkaConsumerLag, gaps, duplicates,
		persistenceChecked, kafkaLagChecked, metricsAvailable, metrics)
	encoded, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(encoded))
	if *out != "" {
		if err := os.WriteFile(*out, append(encoded, '\n'), 0644); err != nil {
			log.Fatalf("write result: %v", err)
		}
	}
}

func loadUsers(path string) []user {
	var data []byte
	if raw := os.Getenv("KAMA_PERF_USERS"); raw != "" {
		data = []byte(raw)
	} else {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			log.Fatal(err)
		}
	}
	var users []user
	if err := json.Unmarshal(data, &users); err != nil {
		log.Fatal(err)
	}
	return users
}

func authenticateUsers(users []user, wsURL string) []user {
	endpoint, err := url.Parse(wsURL)
	if err != nil {
		log.Fatal(err)
	}
	if endpoint.Scheme == "wss" {
		endpoint.Scheme = "https"
	} else if endpoint.Scheme == "ws" {
		endpoint.Scheme = "http"
	}
	endpoint.Path = "/login"
	endpoint.RawQuery = ""
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // Local test server uses a self-signed certificate.
	for i := range users {
		if users[i].Token != "" {
			continue
		}
		if users[i].loginID() == "" || users[i].Password == "" {
			log.Fatal("every user needs a token or email/telephone/password credentials")
		}
		// 当前 login 接口接受 email；若只有 telephone 则复用 telephone 字段值作为 email 登录
		loginReq := map[string]string{"password": users[i].Password}
		if users[i].Email != "" {
			loginReq["email"] = users[i].Email
		} else {
			loginReq["email"] = users[i].Telephone
		}
		body, _ := json.Marshal(loginReq)
		response, err := httpClient.Post(endpoint.String(), "application/json", bytes.NewReader(body))
		if err != nil {
			log.Fatalf("login %s: %v", users[i].loginID(), err)
		}
		var payload struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    user   `json:"data"`
		}
		err = json.NewDecoder(response.Body).Decode(&payload)
		response.Body.Close()
		if err != nil || (payload.Code != 0 && payload.Code != http.StatusOK) || payload.Data.Token == "" {
			log.Fatalf("login %s failed: %s (%v)", users[i].loginID(), payload.Message, err)
		}
		users[i].UUID = payload.Data.UUID
		users[i].Token = payload.Data.Token
		if users[i].Nickname == "" {
			users[i].Nickname = payload.Data.Nickname
		}
	}
	seen := make(map[string]bool)
	valid := make([]user, 0, len(users))
	for _, u := range users {
		if u.UUID == "" || u.Token == "" || seen[u.UUID] {
			continue
		}
		seen[u.UUID] = true
		if u.Nickname == "" {
			u.Nickname = u.UUID
		}
		valid = append(valid, u)
	}
	return valid
}

func readDeliveries(conn *websocket.Conn, myUUID string, mu *sync.Mutex, pending map[string]pendingDelivery, receiverLatencies *[]time.Duration, received *int64, lastReceivedAt *int64, total int, done chan struct{}, once *sync.Once, tracker *seqTracker) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var receivedMessage struct {
			Content string `json:"content"`
		}
		if json.Unmarshal(raw, &receivedMessage) != nil || !strings.HasPrefix(receivedMessage.Content, "perf:") {
			continue
		}
		mu.Lock()
		delivery, ok := pending[receivedMessage.Content]
		isExpectedReceiver := ok && delivery.receiverUUID == myUUID
		if isExpectedReceiver {
			delete(pending, receivedMessage.Content)
			*receiverLatencies = append(*receiverLatencies, time.Since(delivery.startedAt))
		}
		mu.Unlock()

		if isExpectedReceiver {
			_, senderUUID, seq, parsed := parsePerfContent(receivedMessage.Content)
			if parsed {
				tracker.record(senderUUID, seq)
			}
		}
		if isExpectedReceiver && int(atomic.AddInt64(received, 1)) == total {
			once.Do(func() { close(done) })
		}
		if isExpectedReceiver {
			atomic.StoreInt64(lastReceivedAt, time.Now().UnixNano())
		}
	}
}

func buildResult(runID string, started, writesFinished, finished, lastReceived time.Time, completed bool, users, ratePerUser, requested, received int, connectionErrors, writeErrors int64, receiverLatencies []time.Duration, msgPersisted int, kafkaTopicEndOffsets int64, kafkaConsumerLag int64, gaps, duplicates int, persistenceChecked, kafkaLagChecked, metricsCollected bool, metrics metricsSnapshot) result {
	sort.Slice(receiverLatencies, func(i, j int) bool { return receiverLatencies[i] < receiverLatencies[j] })
	duration := finished.Sub(started)
	sendDuration := writesFinished.Sub(started)
	deliveryDuration := time.Duration(0)
	if received > 0 && !lastReceived.IsZero() {
		deliveryDuration = lastReceived.Sub(started)
	}
	metric := func(percentile float64) float64 {
		if len(receiverLatencies) == 0 {
			return 0
		}
		index := int(float64(len(receiverLatencies)-1) * percentile)
		return float64(receiverLatencies[index].Microseconds()) / 1000
	}
	r := result{
		RunID:     runID,
		StartedAt: started, FinishedAt: finished, Users: users, RatePerUser: ratePerUser,
		MessagesRequested: requested, MessagesReceived: received,
		MessagesPersisted: msgPersisted, PersistenceChecked: persistenceChecked,
		KafkaLag: kafkaTopicEndOffsets, KafkaConsumerLag: kafkaConsumerLag, KafkaLagChecked: kafkaLagChecked,
		Duplicates: duplicates, SequenceGaps: gaps,
		ChannelRouted: metrics.ChannelRouted, KafkaRouted: metrics.KafkaRouted, BatchFlushErrors: metrics.BatchFlushErrors,
		DeliveryQueueTimeouts: metrics.DeliveryQueueTimeouts, DeliveryClientClosed: metrics.DeliveryClientClosed,
		DeliveryStatusQueueDrops: metrics.DeliveryStatusQueueDrops, SessionQueueTimeouts: metrics.SessionQueueTimeouts,
		ProcessFailures: metrics.ProcessFailures, KafkaCommitFailures: metrics.KafkaCommitFailures,
		MetricsCollected: metricsCollected,
		ConnectionErrors: connectionErrors, WriteErrors: writeErrors,
		DurationMS:         float64(duration.Microseconds()) / 1000,
		SendDurationMS:     float64(sendDuration.Microseconds()) / 1000,
		DeliveryDurationMS: float64(deliveryDuration.Microseconds()) / 1000,
		Completed:          completed, TimedOut: !completed,
		P50MS: metric(0.50), P95MS: metric(0.95), P99MS: metric(0.99),
	}
	if deliveryDuration > 0 {
		r.Throughput = float64(received) / deliveryDuration.Seconds()
	}
	if sendDuration > 0 {
		r.SendThroughput = float64(requested) / sendDuration.Seconds()
	}
	if requested > 0 {
		r.SuccessRate = float64(received) / float64(requested) * 100
	}
	return r
}

// queryDBRowCount counts messages belonging to the given runID.
func queryDBRowCount(dsn, runID string, since time.Time) (int, bool) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("DB open: %v", err)
		return 0, false
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM `message` WHERE content LIKE ? AND created_at >= ?",
		fmt.Sprintf("perf:%s:%%", runID), since.Add(-1*time.Second)).Scan(&count); err != nil {
		log.Printf("DB query: %v", err)
		return 0, false
	}
	return count, true
}

func waitForPersisted(dsn, runID string, since time.Time, expected int, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for {
		count, ok := queryDBRowCount(dsn, runID, since)
		if !ok || count >= expected || time.Now().After(deadline) {
			return count, ok
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// metricsSnapshot holds the subset of /metrics fields we care about.
type metricsSnapshot struct {
	ChannelRouted            uint64 `json:"channel_routed"`
	KafkaRouted              uint64 `json:"kafka_routed"`
	BatchFlushErrors         uint64 `json:"batch_flush_errors"`
	DeliveryQueueTimeouts    uint64 `json:"delivery_queue_timeouts"`
	DeliveryClientClosed     uint64 `json:"delivery_client_closed"`
	DeliveryStatusQueueDrops uint64 `json:"delivery_status_queue_drops"`
	SessionQueueTimeouts     uint64 `json:"session_queue_timeouts"`
	ProcessFailures          uint64 `json:"process_failures"`
	KafkaCommitFailures      uint64 `json:"kafka_commit_failures"`
}

func (m metricsSnapshot) subtract(before metricsSnapshot) metricsSnapshot {
	subtract := func(after, prior uint64) uint64 {
		if after < prior {
			return 0 // The server restarted during the run; do not underflow.
		}
		return after - prior
	}
	return metricsSnapshot{
		ChannelRouted:            subtract(m.ChannelRouted, before.ChannelRouted),
		KafkaRouted:              subtract(m.KafkaRouted, before.KafkaRouted),
		BatchFlushErrors:         subtract(m.BatchFlushErrors, before.BatchFlushErrors),
		DeliveryQueueTimeouts:    subtract(m.DeliveryQueueTimeouts, before.DeliveryQueueTimeouts),
		DeliveryClientClosed:     subtract(m.DeliveryClientClosed, before.DeliveryClientClosed),
		DeliveryStatusQueueDrops: subtract(m.DeliveryStatusQueueDrops, before.DeliveryStatusQueueDrops),
		SessionQueueTimeouts:     subtract(m.SessionQueueTimeouts, before.SessionQueueTimeouts),
		ProcessFailures:          subtract(m.ProcessFailures, before.ProcessFailures),
		KafkaCommitFailures:      subtract(m.KafkaCommitFailures, before.KafkaCommitFailures),
	}
}

// queryMetrics fetches route counters from the server's /metrics endpoint.
func queryMetrics(url string) (metricsSnapshot, bool) {
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Metrics query: %v", err)
		return metricsSnapshot{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Metrics query: unexpected status %s", resp.Status)
		return metricsSnapshot{}, false
	}
	var m metricsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		log.Printf("Metrics decode: %v", err)
		return metricsSnapshot{}, false
	}
	return m, true
}

// queryKafkaLag returns (topicEndOffsets, consumerLag).
// topicEndOffsets = sum of last offsets across partitions (total messages ever written).
// consumerLag = topicEndOffsets - sum of committed offsets for the given consumer group.
func queryKafkaLag(broker, topic, group string) (int64, int64) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		log.Printf("Kafka dial: %v", err)
		return -1, -1
	}
	partitions, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		log.Printf("Kafka read partitions: %v", err)
		return -1, -1
	}

	partitionIDs := make([]int, 0, len(partitions))
	topicEndOffsets := make(map[int]int64, len(partitions))

	for _, p := range partitions {
		partitionIDs = append(partitionIDs, p.ID)
		// Get the last offset (high watermark) for this partition
		c, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, p.ID)
		if err != nil {
			log.Printf("Kafka dial leader for partition %d: %v", p.ID, err)
			return -1, -1
		}
		last, err := c.ReadLastOffset()
		c.Close()
		if err != nil {
			log.Printf("Kafka read last offset for partition %d: %v", p.ID, err)
			return -1, -1
		}
		topicEndOffsets[p.ID] = last
	}

	addr, err := net.ResolveTCPAddr("tcp", broker)
	if err != nil {
		log.Printf("Kafka resolve broker: %v", err)
		return sumOffsets(topicEndOffsets), -1
	}
	client := &kafka.Client{Addr: addr, Timeout: 5 * time.Second}
	offsetResponse, err := client.OffsetFetch(context.Background(), &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: partitionIDs},
	})
	if err != nil || offsetResponse.Error != nil {
		if err == nil {
			err = offsetResponse.Error
		}
		log.Printf("Kafka offset fetch: %v", err)
		return sumOffsets(topicEndOffsets), -1
	}

	committedOffsets := make(map[int]int64, len(partitionIDs))
	for _, partition := range offsetResponse.Topics[topic] {
		if partition.Error != nil {
			log.Printf("Kafka offset fetch partition %d: %v", partition.Partition, partition.Error)
			return sumOffsets(topicEndOffsets), -1
		}
		committedOffsets[partition.Partition] = partition.CommittedOffset
	}
	return sumOffsets(topicEndOffsets), calculateConsumerLag(topicEndOffsets, committedOffsets)
}

func waitForKafkaLag(broker, topic, group string, timeout time.Duration) (int64, int64, bool) {
	deadline := time.Now().Add(timeout)
	lastTopicEndOffsets, lastLag := int64(-1), int64(-1)
	checked := false
	for {
		topicEndOffsets, consumerLag := queryKafkaLag(broker, topic, group)
		if topicEndOffsets >= 0 && consumerLag >= 0 {
			lastTopicEndOffsets, lastLag, checked = topicEndOffsets, consumerLag, true
			if consumerLag == 0 {
				return topicEndOffsets, consumerLag, true
			}
		}
		if time.Now().After(deadline) {
			return lastTopicEndOffsets, lastLag, checked
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func sumOffsets(offsets map[int]int64) int64 {
	var total int64
	for _, offset := range offsets {
		total += offset
	}
	return total
}

func calculateConsumerLag(topicEndOffsets, committedOffsets map[int]int64) int64 {
	var lag int64
	for partition, endOffset := range topicEndOffsets {
		committedOffset := committedOffsets[partition]
		if committedOffset < 0 {
			committedOffset = 0
		}
		if endOffset > committedOffset {
			lag += endOffset - committedOffset
		}
	}
	return lag
}
