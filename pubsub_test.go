package pubsub_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/augustjourney/pubsub"
)

func recv[T any](t *testing.T, ch <-chan T, timeout time.Duration) (T, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(timeout):
		var zero T
		return zero, false
	}
}

func TestPublishSubscribe(t *testing.T) {
	bus := pubsub.New[string](5)
	defer bus.Shutdown()

	ch := bus.Subscribe("news")
	bus.Publish("news", "hello")

	got, ok := recv(t, ch, 100*time.Millisecond)
	if !ok || got != "hello" {
		t.Fatalf("expected hello, got %q ok=%v", got, ok)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	bus := pubsub.New[int](5)
	defer bus.Shutdown()

	subs := []<-chan int{
		bus.Subscribe("t"),
		bus.Subscribe("t"),
		bus.Subscribe("t"),
	}
	bus.Publish("t", 42)

	for i, ch := range subs {
		got, ok := recv(t, ch, 100*time.Millisecond)
		if !ok || got != 42 {
			t.Fatalf("subscriber %d: expected 42, got %d ok=%v", i, got, ok)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := pubsub.New[string](5)
	defer bus.Shutdown()

	ch := bus.Subscribe("t")
	bus.Unsubscribe("t", ch)

	if _, ok := recv(t, ch, 50*time.Millisecond); ok {
		t.Fatal("channel should be closed after Unsubscribe")
	}

	bus.Publish("t", "no panic please")
}

func TestUnsubscribeNonexistent(t *testing.T) {
	bus := pubsub.New[string](5)
	defer bus.Shutdown()

	bus.Unsubscribe("missing", nil)
	bus.Unsubscribe("missing", make(chan string))

	ch := bus.Subscribe("t")
	other := make(chan string)
	bus.Unsubscribe("t", other)

	bus.Publish("t", "still subscribed")
	got, ok := recv(t, ch, 100*time.Millisecond)
	if !ok || got != "still subscribed" {
		t.Fatalf("expected message, got %q ok=%v", got, ok)
	}
}

func TestShutdown(t *testing.T) {
	bus := pubsub.New[string](5)
	ch := bus.Subscribe("t")

	bus.Shutdown()

	if _, ok := recv(t, ch, 50*time.Millisecond); ok {
		t.Fatal("channel should be closed after Shutdown")
	}
	if got := bus.Subscribe("t"); got != nil {
		t.Fatal("Subscribe after Shutdown must return nil")
	}
	bus.Publish("t", "after-shutdown")
}

func TestShutdownIdempotent(t *testing.T) {
	bus := pubsub.New[string](5)
	bus.Subscribe("t")

	bus.Shutdown()
	bus.Shutdown()
	bus.Shutdown()
}

func TestPublishToUnknownTopic(t *testing.T) {
	bus := pubsub.New[string](5)
	defer bus.Shutdown()

	done := make(chan struct{})
	go func() {
		bus.Publish("unknown", "x")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish to unknown topic must not block")
	}
}

func TestSlowConsumer(t *testing.T) {
	bus := pubsub.New[int](1)
	defer bus.Shutdown()

	bus.Subscribe("t")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			bus.Publish("t", i)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish must not block on full buffer")
	}
}

func TestRaceShutdownVsPublish(t *testing.T) {
	bus := pubsub.New[int](4)

	for i := 0; i < 16; i++ {
		bus.Subscribe("t")
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				bus.Publish("t", j)
			}
		}()
	}

	time.Sleep(time.Millisecond)
	bus.Shutdown()
	wg.Wait()
}

func TestConcurrent(t *testing.T) {
	bus := pubsub.New[int](16)
	defer bus.Shutdown()

	const subs = 8
	const pubs = 8
	const msgs = 200

	var received atomic.Int64
	var rwg sync.WaitGroup
	for i := 0; i < subs; i++ {
		ch := bus.Subscribe("t")
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for range ch {
				received.Add(1)
			}
		}()
	}

	var pwg sync.WaitGroup
	for i := 0; i < pubs; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for j := 0; j < msgs; j++ {
				bus.Publish("t", j)
			}
		}()
	}
	pwg.Wait()

	bus.Shutdown()
	rwg.Wait()

	if received.Load() == 0 {
		t.Fatal("expected at least some messages delivered")
	}
}

func TestGenericTypes(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		bus := pubsub.New[int](2)
		defer bus.Shutdown()
		ch := bus.Subscribe("n")
		bus.Publish("n", 7)
		if v, ok := recv(t, ch, 100*time.Millisecond); !ok || v != 7 {
			t.Fatalf("got %d ok=%v", v, ok)
		}
	})

	t.Run("string", func(t *testing.T) {
		bus := pubsub.New[string](2)
		defer bus.Shutdown()
		ch := bus.Subscribe("s")
		bus.Publish("s", "hi")
		if v, ok := recv(t, ch, 100*time.Millisecond); !ok || v != "hi" {
			t.Fatalf("got %q ok=%v", v, ok)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type event struct {
			ID   int
			Name string
		}
		bus := pubsub.New[event](2)
		defer bus.Shutdown()
		ch := bus.Subscribe("e")
		want := event{ID: 1, Name: "test"}
		bus.Publish("e", want)
		if v, ok := recv(t, ch, 100*time.Millisecond); !ok || v != want {
			t.Fatalf("got %+v ok=%v", v, ok)
		}
	})
}
