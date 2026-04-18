package main

// import (
// 	"errors"
// 	"fmt"
// 	"sync"
// 	"time"
// )

// type Message struct {
// 	ID int
// 	Value int
// }

// type Filter func(Message) bool

// type Broker struct {
// 	ch chan Message
// }

// type Consumer struct {
// 	name string
// 	in <-chan Message
// 	pending map[int]Message
// 	mu sync.Mutex
// 	wg *sync.WaitGroup
// }

// func NewConsumer(name string,in <-chan Message,wg *sync.WaitGroup) *Consumer {
// 	return &Consumer{
// 		name: name,
// 		in: in,
// 		pending: make(map[int]Message),
// 		wg: wg,
// 	}
// }

// func (c *Consumer) Start() {
// 	defer c.wg.Done()

// 	for msg := range c.in {
// 		c.mu.Lock()
// 		time.Sleep(time.Second * 3)
// 		c.pending[msg.ID] = msg
// 		c.mu.Unlock()

// 		fmt.Println("Consumer", c.name, "received:", msg)
// 	}
// }

// func (c *Consumer) Ack(id int) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	delete(c.pending, id)
// 	fmt.Println("Consumer", c.name, "acked message id:", id)
// }

// func (c *Consumer) PrintPending() {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	fmt.Println("pending of", c.name)
// 	for id,msg := range c.pending {
// 		fmt.Println("id:", id, "value:", msg.Value)
// 	}
// 	fmt.Println("--------")
// }

// func Producer() {
// 	var wg sync.WaitGroup
// 	broker := NewBroker()
// 	consumer := NewConsumer("c1", broker.ch, &wg)

// 	filters := []Filter{
// 		EvenFilter,
// 		NotZeroFilter,
// 		MinFilter(100),
// 		MaxFilter(300),
// 	}

// 	wg.Add(1)
// 	go consumer.Start()

// 	for i := 1; i <= 4; i++ {
// 		msg := Message{ID: i, Value: i* 100}

// 		if ApplyFilters(msg, filters) {
// 			fmt.Println("before send:", i, "value:", msg.Value)
// 			err := broker.Publish(msg)
// 			if err != nil {
// 				fmt.Println("message dropped:", err)
// 			}
// 			fmt.Println("after send:", i, "value:", msg.Value)
// 		} else {
// 			fmt.Println("message does not pass filters:", msg.Value)
// 		}

// 	}

// 	broker.Close()
// 	wg.Wait()

// 	consumer.PrintPending()

// 	// consumer.Ack(2)

// 	// consumer.PrintPending()

// }

// func NewBroker() *Broker {
// 	ch := make(chan Message, 1)
// 	return &Broker{
// 		ch: ch,
// 	}
// }

// func (b *Broker) Publish(msg Message) error {
// 	select {
// 	case b.ch <- msg:
// 		return nil
// 	default:
// 		return errors.New("channel is full")
// 	}
// }
// func (b* Broker) Close() {
// 	close(b.ch)
// }

// func EvenFilter(msg Message) bool {
// 	return msg.Value % 2 == 0
// }

// func NotZeroFilter(msg Message) bool {
// 	return msg.Value != 0
// }

// func MinFilter(min int) Filter {
// 	return func(msg Message) bool {
// 		return msg.Value >= min
// 	}
// }

// func MaxFilter(max int) Filter {
// 	return func(msg Message) bool {
// 		return msg.Value <= max
// 	}
// }

// func ApplyFilters(msg Message, filters []Filter) bool {
// 	for _, filter := range filters {
// 		if !filter(msg) {
// 			return false
// 		}
// 	}
// 	return true
// }

