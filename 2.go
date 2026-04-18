package main

// import (
// 	"fmt"
// 	"sync"
// )

// type Broker struct {
// 	ch1 chan int
// 	ch2 chan int
// }

// func (b *Broker) Publish(n int) {
// 	b.ch1 <- n
// 	b.ch2 <- n
// }

// func (b *Broker) Close() {
// 	close(b.ch1)
// 	close(b.ch2)
// }

// func Producer() {
// 	var wg sync.WaitGroup
// 	b := &Broker{
// 		ch1: make(chan int),
// 		ch2: make(chan int),
// 	}

// 	wg.Add(2)

// 	go Consumer("c1", b.ch1, &wg)
// 	go Consumer("c2", b.ch2, &wg)

// 	for i := 1; i < 6; i++ {
// 		b.Publish(i)
// 	}

// 	b.Close()

// 	wg.Wait()
// }

// func Consumer(c string,ch <-chan int,wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	for msg := range ch {
// 		fmt.Println("Consumer", c, "received:", msg)
// 	}
// }