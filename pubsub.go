package pubsub

import (
	"slices"
	"sync"
	"sync/atomic"
)

const defaultBufSize = 16

// PubSub — потокобезопасная in-memory pub/sub шина.
type PubSub[T any] struct {
	mu      sync.RWMutex
	topics  map[string][]chan T
	stopped atomic.Bool
	bufSize int
	once    sync.Once
}

// New создаёт PubSub.
//
// Параметр bufSize задаёт размер буфера канала каждого
// подписчика. При bufSize <= 0 используется значение по умолчанию.
func New[T any](bufSize int) *PubSub[T] {
	if bufSize <= 0 {
		bufSize = defaultBufSize
	}
	return &PubSub[T]{
		topics:  map[string][]chan T{},
		bufSize: bufSize,
	}
}

// Subscribe регистрирует нового подписчика на топик и возвращает канал,
// в который будут приходить сообщения.
//
// После Shutdown возвращает nil.
func (p *PubSub[T]) Subscribe(topic string) <-chan T {
	if p.stopped.Load() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped.Load() {
		return nil
	}
	ch := make(chan T, p.bufSize)
	p.topics[topic] = append(p.topics[topic], ch)
	return ch
}

// Unsubscribe удаляет подписку и закрывает её канал.
//
// Безопасен для повторных вызовов и для каналов,
// которые уже не зарегистрированы — в этих случаях это no-op.
func (p *PubSub[T]) Unsubscribe(topic string, channel <-chan T) {
	if channel == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	subs, ok := p.topics[topic]
	if !ok {
		return
	}
	for idx, ch := range subs {
		if ch == channel {
			p.topics[topic] = slices.Delete(subs, idx, idx+1)
			close(ch)
			break
		}
	}
	if len(p.topics[topic]) == 0 {
		delete(p.topics, topic)
	}
}

// Publish рассылает сообщение всем подписчикам топика.
//
// Семантика best-effort: если буфер подписчика заполнен, сообщение для него отбрасывается,
// чтобы не блокировать остальных.
//
// Публикация в неизвестный топик — no-op.
func (p *PubSub[T]) Publish(topic string, msg T) {
	if p.stopped.Load() {
		return
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped.Load() {
		return
	}
	subs, ok := p.topics[topic]
	if !ok {
		return
	}
	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Shutdown останавливает PubSub: помечает его как закрытый, закрывает все
// каналы подписчиков и очищает внутреннее состояние.
// Идемпотентен — повторные вызовы безопасны.
func (p *PubSub[T]) Shutdown() {
	p.once.Do(func() {
		p.stopped.Store(true)
		p.mu.Lock()
		defer p.mu.Unlock()
		for topic, subs := range p.topics {
			for _, ch := range subs {
				close(ch)
			}
			delete(p.topics, topic)
		}
	})
}
