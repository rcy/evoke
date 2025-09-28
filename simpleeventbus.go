package evoke

import (
	"fmt"
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

func (b *simpleEventBus) Subscribe(evt Event, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[TypeName(evt)] = append(b.subscribers[TypeName(evt)], handler)
}

func (b *simpleEventBus) Publish(evt RecordedEvent, replay bool) error {
	b.mu.RLock()
	handlers, ok := b.subscribers[TypeName(evt.Event)]
	b.mu.RUnlock()
	if !ok {
		fmt.Printf("WARN: simpleEventBus.Publish: no subscriptions on %T\n", evt.Event)
		return nil
	}
	for _, h := range handlers {
		err := h.Handle(evt.Event, replay)
		if err != nil {
			return err
		}
	}
	return nil
}
