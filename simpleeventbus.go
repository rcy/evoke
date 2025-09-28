package evoke

import (
	"sync"
)

type simpleEventBus struct {
	subscribers map[string][]EventHandler
	mu          sync.RWMutex
}

func NewEventBus() *simpleEventBus {
	return &simpleEventBus{
		subscribers: make(map[string][]EventHandler),
	}
}

func (b *simpleEventBus) Subscribe(eventType string, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[eventType] = append(b.subscribers[eventType], handler)
}

func (b *simpleEventBus) Publish(evt RecordedEvent, replay bool) error {
	b.mu.RLock()
	handlers := b.subscribers[TypeName(evt.Event)]
	b.mu.RUnlock()
	for _, h := range handlers {
		err := h.Handle(evt.Event, replay)
		if err != nil {
			return err
		}
	}
	return nil
}
