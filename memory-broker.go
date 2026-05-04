package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)
type Message struct {
	ID    uint64
	Value int
}

type Filter func(m Message) bool

type Producer interface {
	Publish(value int) error

	WithFilter(f Filter) Producer

	WithZeroFilter() Producer
	WithEvenFilter() Producer
	WithOddFilter() Producer
	WithMinMaxFilter(min, max int) Producer

	Close()
}

type Consumer interface {
	Messages() <-chan Message

	Ack(id uint64) error

	Pending() []Message

	Close()
}

type Queue interface {
	Name() string

	Subscribe() Consumer

	ConsumerCount() int
}

type Broker interface {
	Queue(name string) Queue

	NewProducer(queueName string) Producer

	Close()
}

// Обхяить интерфейсы как в задании - 1
type broker struct {
	queues map[string]*queue
	closed bool
	mu sync.Mutex
}

type producer struct {
	queue *queue
	filters []Filter
	closed bool
	mu sync.Mutex
}

type queue struct {
	name string
	input chan Message
	done chan struct{}
	consumers []*consumer
	closed bool
	nextConsumerID atomic.Uint64
	nextMsgID atomic.Uint64
	mu sync.Mutex
	wg sync.WaitGroup
}

type consumer struct {
	id uint64
	queue *queue
	ch chan Message
	pending map[uint64]Message
	closed bool
	mu sync.Mutex
}

func NewBroker() Broker {
	return &broker{
		queues: make(map[string]*queue),
		closed: false,
	}
}

func (b *broker) Close() {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return
	}

	b.closed = true
	queues := make([]*queue, 0, len(b.queues))

	for _, q := range b.queues {
		queues = append(queues, q)
	}
	b.mu.Unlock()

	for _, q := range queues {
		q.Close()
	}
}

func (b *broker) NewProducer(queueName string) Producer {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	q, exists := b.queues[queueName]
	if !exists {
		q = newQueue(queueName)
		b.queues[queueName] = q
	}

	p := &producer{
		queue: q,
		filters: make([]Filter, 0),
		closed: false,
	}
	return p
}

func (b *broker) Queue(name string) Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	if q, exists := b.queues[name]; exists {
		return q
	}

	q := newQueue(name)

	b.queues[name] = q
	return q
}

func newQueue(name string) *queue {
	q := &queue{
		name: name,
		// Размер 100: Publish() не блокируется, при переполнении возвращает ошибку
		input: make(chan Message, 100),
		done: make(chan struct{}),
		consumers: make([]*consumer, 0),
		closed: false,
	}
	go q.run()
	return q
}


func (q *queue) Subscribe() Consumer {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	
	ch := make(chan Message, 100)
	
	consumer := &consumer{
		id: q.nextConsumerID.Add(1),
		ch: ch,
		pending: make(map[uint64]Message),
		queue: q,
		closed: false,
	}
	q.consumers = append(q.consumers, consumer)
	return consumer
}

func (q *queue) publish(msg Message) error{
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()

	if closed {
		return errors.New("queue is closed")
	}

	select {
	case q.input <- msg:
		return nil
	default:
		return errors.New("queue is full")
	}
}

func (q *queue) dispatch(msg Message) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return 
	}

	activeConsumers := make([]*consumer, 0, len(q.consumers))

	for _, c := range q.consumers {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if !closed {
			activeConsumers = append(activeConsumers, c)
		}
	}
	q.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range activeConsumers {
		wg.Add(1)
		go func(consumer *consumer) {
			defer wg.Done()
			consumer.mu.Lock()
			defer consumer.mu.Unlock()
			
			if consumer.closed {
				return
			}
			
			select {
			case consumer.ch <- msg:
				consumer.pending[msg.ID] = msg
			default:
			}
		}(c)
	}
	wg.Wait()
}

func(q *queue) run() {
	for {
		select {
		case msg, ok := <- q.input:
			if !ok {
				return 
			}
			q.wg.Add(1)
			q.dispatch(msg)
			q.wg.Done()
		case <- q.done:	
			return
		}
	}
}

func (q *queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}

	q.closed = true
	consumers := make([]*consumer,len(q.consumers))
	copy(consumers, q.consumers)

	q.consumers = nil
	q.mu.Unlock()

	close(q.done)
	q.wg.Wait()

	for _, c := range consumers {
		c.mu.Lock()
		if !c.closed {
			c.closed = true
			close(c.ch)
		}
		c.mu.Unlock()
	}
}

func (q *queue) removeConsumer(target *consumer) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	
	var removed *consumer
    for i, c := range q.consumers {
		if c.id == target.id {
			removed = c
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}
	q.mu.Unlock()

	if removed == nil {
		return
	}
	removed.mu.Lock()
	if !removed.closed {
		removed.closed = true
		close(removed.ch)
	}
	removed.mu.Unlock()
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) ConsumerCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.consumers)
}

func (c *consumer) Ack(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.pending[id]; !exists {
		return fmt.Errorf("Message with id %d not found", id)
	}

	delete(c.pending, id)
	return nil
}

func (c *consumer) Messages() <-chan Message {
	return c.ch
}

func (c *consumer) Pending() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]Message, 0, len(c.pending))
	for _, msg := range c.pending {
		result = append(result, msg)
	}

	return result
}

func (c *consumer) Close() {
	if c.queue == nil {
		return 
	}
	c.queue.removeConsumer(c)
}

func (p *producer) Publish(value int) error{
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("producer closed")
	}
	id := p.queue.nextMsgID.Add(1)
	msg := Message{ID: id, Value: value}

	if !ApplyFilters(msg, p.filters) {
		return nil
	}

	return p.queue.publish(msg)
}

func (p *producer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

func (p *producer) WithFilter(f Filter) Producer {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.filters = append(p.filters, f)
	return p
}

func (p *producer) WithZeroFilter() Producer {
	return p.WithFilter(NotZeroFilter)
}

func (p *producer) WithEvenFilter() Producer {
	return p.WithFilter(EvenFilter)
}

func (p *producer) WithOddFilter() Producer {
	return p.WithFilter(OddFilter)
}

func (p *producer) WithMinMaxFilter(min, max int) Producer {
	return p.WithFilter(func(msg Message) bool {
		return msg.Value >= min && msg.Value <= max
	})
}


// filters 
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

