package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	broker := NewBroker()
	defer broker.Close()

	// создаем продюсер с фильтрами
	producer := broker.NewProducer("orders")
	producer.WithEvenFilter().WithMinMaxFilter(100,300)

	// создаем и подписываем консюмеров
	consumer1 := broker.Queue("orders").Subscribe()
	consumer2 := broker.Queue("orders").Subscribe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func()  {
		defer wg.Done()
		for msg := range consumer1.Messages() {
			fmt.Println("consumer1 received:", msg)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range consumer2.Messages() {
			fmt.Println("consumer2 received:", msg)

			consumer2.Ack(msg.ID)
		}
	}()

	producer.Publish(200)
	producer.Publish(120)
	producer.Publish(150)

	time.Sleep(100 * time.Millisecond)

	broker.Close()
	wg.Wait()

	fmt.Println("Consumer pending:", consumer1.Pending())
	fmt.Println("Consumer pending:", consumer2.Pending())
}



