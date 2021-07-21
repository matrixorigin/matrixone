package main

import (
	"time"
)

func main() {
	completedC := make(chan int, 1)
	defer close(completedC)

	go func() {
		time.Sleep(1 * time.Second)
		completedC <- 1
	}()

	select {
	case <-completedC:
		println("[QCompletedC]")
	case <-time.After(3 * time.Second):
		println("[QTimeout]")
	}

	println("QQQQQQ")
}
