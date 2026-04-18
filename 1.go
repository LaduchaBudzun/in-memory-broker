package main

// import "fmt"

// func Producer() {
// 	ch := make(chan int)

// 	go Consumer(ch)

// 	for i := 1; i < 6; i++ {
// 		ch <- i
// 	}

// 	close(ch)
// }

// func Consumer(ch <-chan int) {
// 	for msg := range ch {
// 		fmt.Println("Received:", msg)
// 	}
// }