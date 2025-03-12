package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/augustjourney/pubsub"
)

func main() {
	bus := pubsub.New[string](5)
	defer bus.Shutdown()

	news := bus.Subscribe("news")
	sport := bus.Subscribe("sport")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for msg := range news {
			fmt.Println("news:", msg)
		}
	}()

	go func() {
		defer wg.Done()
		for msg := range sport {
			fmt.Println("sport:", msg)
		}
	}()

	bus.Publish("news", "rocket launched")
	bus.Publish("sport", "match starts")
	bus.Publish("news", "election results")
	bus.Publish("unknown", "no subscribers")

	time.Sleep(50 * time.Millisecond)

	bus.Unsubscribe("news", news)
	bus.Unsubscribe("sport", sport)

	wg.Wait()
}
