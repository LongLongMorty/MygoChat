package chat

import (
	"sync"
	"testing"

	"kama_chat_server/internal/service/transport"
)

type fakePresence struct {
	locations map[string]string
	instances []string
}

func (f *fakePresence) Register(string) error   { return nil }
func (f *fakePresence) Unregister(string) error { return nil }
func (f *fakePresence) RegisterInstance() error { return nil }
func (f *fakePresence) LiveInstances() ([]string, error) {
	return f.instances, nil
}
func (f *fakePresence) Lookup(u string) (string, bool) {
	v, ok := f.locations[u]
	return v, ok
}
func (f *fakePresence) LookupMany(us []string) (map[string]string, error) {
	m := make(map[string]string)
	for _, u := range us {
		if v, ok := f.locations[u]; ok {
			m[u] = v
		}
	}
	return m, nil
}

type fakePublisher struct {
	published map[string][]*transport.BusMessage
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{published: make(map[string][]*transport.BusMessage)}
}

func (f *fakePublisher) Publish(instance string, msg *transport.BusMessage) error {
	f.published[instance] = append(f.published[instance], msg)
	return nil
}

func newTestClient(uuid string) *Client {
	return &Client{
		Uuid:     uuid,
		SendBack: make(chan *MessageBack, 16),
		done:     make(chan struct{}),
	}
}

func testDistributor(clients map[string]*Client) *Distributor {
	return NewDistributor(clients, &sync.RWMutex{})
}

func TestDistributorDisabledStaysLocal(t *testing.T) {
	local := newTestClient("U-local")
	clients := map[string]*Client{"U-local": local}
	d := testDistributor(clients)

	d.Deliver([]string{"U-local", "U-remote"}, &MessageBack{Message: []byte("hi")}, "M1")

	if got := len(local.SendBack); got != 1 {
		t.Fatalf("local client should receive 1 delivery, got %d", got)
	}
}

func TestDistributorRoutesRemoteByInstance(t *testing.T) {
	local := newTestClient("U-a")
	clients := map[string]*Client{"U-a": local}
	d := testDistributor(clients)

	pres := &fakePresence{locations: map[string]string{
		"U-b": "node-2",
		"U-c": "node-2",
	}}
	pub := newFakePublisher()
	d.Enable("node-1", pres, pub)

	d.Deliver([]string{"U-a", "U-b", "U-c", "U-offline"}, &MessageBack{Message: []byte("hi")}, "M2")

	if got := len(local.SendBack); got != 1 {
		t.Fatalf("local target should receive 1 delivery, got %d", got)
	}
	envs := pub.published["node-2"]
	if len(envs) != 1 {
		t.Fatalf("remote instance should receive 1 aggregated batch, got %d", len(envs))
	}
	if envs[0].Type != transport.BusTypeDeliver || envs[0].Deliver == nil {
		t.Fatalf("expected deliver envelope, got %+v", envs[0])
	}
	if len(envs[0].Deliver.Targets) != 2 {
		t.Fatalf("batch should aggregate 2 targets, got %d", len(envs[0].Deliver.Targets))
	}
	if string(envs[0].Deliver.Payload) != "hi" || envs[0].Deliver.DedupKey != "M2" {
		t.Fatalf("unexpected payload/dedup: %s / %s", envs[0].Deliver.Payload, envs[0].Deliver.DedupKey)
	}
	if _, ok := pub.published["node-1"]; ok {
		t.Fatalf("should not publish to self")
	}
}

func TestDeliverLocalBatchDedup(t *testing.T) {
	local := newTestClient("U-x")
	clients := map[string]*Client{"U-x": local}
	d := testDistributor(clients)
	d.Enable("node-1", &fakePresence{locations: map[string]string{}}, newFakePublisher())

	dm := &transport.DeliveryMessage{Targets: []string{"U-x"}, Payload: []byte("p"), DedupKey: "M3"}
	if err := d.DeliverLocalBatch(dm); err != nil {
		t.Fatalf("DeliverLocalBatch: %v", err)
	}
	if err := d.DeliverLocalBatch(dm); err != nil {
		t.Fatalf("DeliverLocalBatch second: %v", err)
	}

	if got := len(local.SendBack); got != 1 {
		t.Fatalf("duplicate delivery should be deduped, got %d", got)
	}
}

func TestDeliverLocalBatchNoTargets(t *testing.T) {
	d := testDistributor(map[string]*Client{})
	d.Enable("node-1", &fakePresence{locations: map[string]string{}}, newFakePublisher())
	if err := d.DeliverLocalBatch(&transport.DeliveryMessage{}); err != nil {
		t.Fatalf("empty batch should be no-op, got %v", err)
	}
}
