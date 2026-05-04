package main

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestPublishDoesNotBlockWhenQueueFull(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	
	p := b.NewProducer("test")
	defer p.Close()

	done := make(chan struct{})
	go func () {
		defer close(done)

		for i := 0; i < 1000; i++ {
			_ = p.Publish(i)
		}
	}()

	select {
		case <-done:
		case <- time.After(100 * time.Millisecond):
			t.Fatal("Publish blocked when queue is full")
	}

}

func TestConsumersReceiveSameMessages(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("test")
	defer p.Close()

	c1 := b.Queue("test").Subscribe()
	c2 := b.Queue("test").Subscribe()

	messagesCount := 3

	for i:= 0; i < messagesCount; i++ {
		_ = p.Publish(i)
	}

	messages1 := readN(t, c1.Messages(), messagesCount)
	messages2 := readN(t, c2.Messages(), messagesCount)

	for i := 0; i < messagesCount; i++ {
		if !reflect.DeepEqual(messages1[i],messages2[i]) {
			t.Fatalf("messages are not the same")
			break
		}
	}

}

func readN(t * testing.T, ch <-chan Message, n int) []Message {
	t.Helper()

	res := make([]Message,0, n)
	timeout := time.After(1 * time.Second)

	for len(res) < n {
		select {
		case msg := <- ch:
			res = append(res, msg)
		case <- timeout:
			t.Fatalf("timeout waiting for message %d", len(res) + 1)	
		}
	}
	return res
}

func TestAckOfOneConsumerDoesNotAffectOtherConsumers(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("test")
	defer p.Close()

	c1 := b.Queue("test").Subscribe()
	c2 := b.Queue("test").Subscribe()


	_ = p.Publish(1)
	msg1 := <-c1.Messages()
	_ = <-c2.Messages()

	if len(c1.Pending()) == 0 || len(c2.Pending()) == 0 {
		t.Fatalf("No message in pending")
	}

	_ = c2.Ack(msg1.ID)

	if len(c1.Pending()) == 0 {
		t.Fatalf("Ack of one consumer affect other consumer")
	}

	if len(c2.Pending()) != 0{
		t.Fatalf("Ack does not remove message from pending")
	}

}

func TestFiltersCanBeUsedInDifferentOrders(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p1 := b.NewProducer("q1");
	p2 := b.NewProducer("q2")
	defer p1.Close()
	defer p2.Close()

	p1.WithEvenFilter().WithMinMaxFilter(10, 20).WithZeroFilter()
	p2.WithZeroFilter().WithMinMaxFilter(10, 20).WithEvenFilter()

	c1 := b.Queue("q1").Subscribe()
	c2 := b.Queue("q2").Subscribe()

	msgs := []int{0, 1, 2, 8, 10, 11, 12, 20, 21, 22}
	expectedResult := []int{10, 12, 20}

	for _, msg := range msgs {
		_ = p1.Publish(msg)
		_ = p2.Publish(msg)
	}

	expectedCount := 3

	messages1 := readN(t, c1.Messages(), expectedCount)
	messages2 := readN(t, c2.Messages(), expectedCount)

	if len(messages1) != len(messages2) {
		t.Fatalf("messages count are not the same")
	}

	for i := 0; i < len(messages1); i++ {
		if !(messages1[i].Value == messages2[i].Value && messages1[i].Value == expectedResult[i]) {
			t.Fatalf("messages are not the same")
		}
	}
}

func TestProducerConsumerClose(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("test")
	defer p.Close()

	c1 := b.Queue("test").Subscribe()

	c1.Close()
	_, ok1 := <- c1.Messages()

	if b.Queue("test").ConsumerCount() != 0 || ok1 {
		t.Fatalf("Consumer is not closed after consumer close")
	}

	c2 := b.Queue("test").Subscribe()

	b.Close()
	_, ok2 := <- c2.Messages()

	if ok2 {
		t.Fatalf("Consumer is not closed after broker close")
	}

	if b.Queue("test") != nil {
		t.Fatal("expected nil queue after broker close")
	}

	if b.NewProducer("test") != nil {
		t.Fatal("expected nil producer after broker close")
	}

	if err := p.Publish(2); err == nil {
		t.Fatal("expected error when publishing after broker close")
	}
	
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestProducerSafeForConcurrentPublish(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("test")
	defer p.Close()

	c := b.Queue("test").Subscribe()

	goroutinesCount := 10
	messagesPerGoroutine := 5
	expectedCount := goroutinesCount * messagesPerGoroutine

	var wg sync.WaitGroup
	wg.Add(goroutinesCount)

	for g := 0; g < goroutinesCount; g++ {
		go func(g int) {
			defer wg.Done()

			for i := 0; i < messagesPerGoroutine; i++ {
				value := g * messagesPerGoroutine + i
				if err := p.Publish(value); err != nil {
					t.Errorf("publish failed: %v:", err)
				}
			}
		}(g)
	}

	wg.Wait()

	messages := readN(t, c.Messages(), expectedCount)

	seen := make(map[int]bool, expectedCount)

	for _,msg := range messages {
		if seen[msg.Value] {
			t.Fatalf("duplicate message value %d", msg.Value)
		}
		seen[msg.Value] = true
	}

	if len(seen) != expectedCount {
		t.Fatalf("got %d unique messages, expected %d", len(seen), expectedCount)
	}
	
}

func TestPendingIsReturnCorrectMessages(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("test")
	defer p.Close()

	c := b.Queue("test").Subscribe()

	expected := []int{0, 10, 20, 30, 40}

	for i := 0; i < 5; i++ {
		if err := p.Publish(i * 10); err != nil {
			t.Fatalf("publish failed: %v:", err)
		}
	}

	messages := readN(t, c.Messages(), len(expected))

	pending := c.Pending()

	if len(pending) != len(expected) {
		t.Fatalf("got %d pending messages, want %d", len(pending), len(expected))
	}

	if err := c.Ack(messages[0].ID); err != nil {
		t.Fatalf("ack failed: %v:", err)
	}

	pending = c.Pending()
	if len(pending) != len(expected) - 1 {
		t.Fatalf("got %d pending messages after ack, want %d", len(pending), len(expected) - 1)
	}

	for _, msg := range pending {
		if msg.ID == messages[0].ID {
			t.Fatalf("acked message is still pending")

		}
	}
}