package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Message struct {
	ID uint64
	Value int
}

type Filter func(Message) bool

type Broker struct {
	queues map[string]*Queue
	mu sync.Mutex
}

type Producer struct {
	queue *Queue
	filters []Filter
	closed bool
	nextID atomic.Uint64
	mu sync.Mutex
}

type Queue struct {
	name string 
	consumers []*Consumer
	mu sync.Mutex
}

type Consumer struct {
	ch chan Message
	pending map[uint64]Message
	closed bool
	mu sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for q := range b.queues {
		b.queues[q].Close()
	}
	b.queues = nil
}

func(b *Broker) NewProducer(queueName string) *Producer {
	q := b.Queue(queueName)

	p := &Producer{
		queue: q,
		filters: make([]Filter, 0),
		closed: false,
	}

	p.nextID.Store(0)
	return p
}

func (b *Broker) Queue(name string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q, exists := b.queues[name]; exists {
		return q
	}

	q := &Queue{
		name: name,
		consumers: make([]*Consumer, 0),
	}
	b.queues[name] = q
	return q
}

func (q *Queue) Subscribe() *Consumer {
	ch := make(chan Message, 100)
	consumer := &Consumer{
		ch: ch,
		pending: make(map[uint64]Message),
		closed: false,
	}

	q.mu.Lock()
	q.consumers = append(q.consumers, consumer)
	q.mu.Unlock()
	return consumer
}

func (q *Queue) publish(msg Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.consumers) == 0 {
		return fmt.Errorf("no consumers")
	}

	var failedCount int

	for _, consumer := range q.consumers {
		if consumer.closed {
			continue
		}
		consumer.mu.Lock()
		consumer.pending[msg.ID] = msg
		consumer.mu.Unlock()

		select {
		case consumer.ch <- msg:
		default:
			failedCount++
		}
	}
	if failedCount == len(q.consumers) {
		return fmt.Errorf("%d consumer(s) buffer full", failedCount)
	}
	return nil
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, consumer := range q.consumers {
		consumer.Close()
	}
	q.consumers = nil
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) ConsumerCount() int {
	return len(q.consumers)
}

func (c *Consumer) Messages() <-chan Message {
	return c.ch
}

func (c *Consumer) Pending() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]Message, 0, len(c.pending))

	for _, msg := range c.pending {
		result = append(result, msg)
	}
	return result
}

func (c *Consumer) Ack(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.pending[id]; !exists {
		return fmt.Errorf("message with id %d not found", id)
	}

	delete(c.pending, id)
	return nil
}

func (c *Consumer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	close(c.ch)
}


func (p *Producer) Publish(value int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("producer closed")
	}

	id := p.nextID.Add(1)
	msg := Message{ID: id, Value: value}

	if !ApplyFilters(msg, p.filters) {
		return fmt.Errorf("message did not pass filters")
	}
	p.queue.publish(msg)
	return nil
}

func (p *Producer) CLose() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

// filters 
func (p *Producer) WithFilter(f Filter) *Producer {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.filters = append(p.filters, f)
	return p
}

func (p *Producer) WithZeroFilter() *Producer {
	return p.WithFilter(NotZeroFilter)
}

func (p *Producer) WithEvenFilter() *Producer {
	return p.WithFilter(EvenFilter)
}

func (p *Producer) WithOddFilter() *Producer {
	return p.WithFilter(OddFilter)
}

func (p *Producer) WithMinMaxFilter(min, max int) *Producer {
	return p.WithFilter(func(msg Message) bool {
		return msg.Value >= min && msg.Value <= max
	})
}



func EvenFilter(msg Message) bool {
	return msg.Value % 2 == 0
}

func NotZeroFilter(msg Message) bool {
	return msg.Value != 0
}

func OddFilter(msg Message) bool {
	return msg.Value % 2 != 0
}

func ApplyFilters(msg Message, filters []Filter) bool {
	for _, filter := range filters {
		if !filter(msg) {
			return false
		}
	}
	return true
}
