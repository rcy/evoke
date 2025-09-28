package evoke

import (
	"fmt"
	"sync"
)

type simpleCommandBus struct {
	handlers map[string]CommandHandler
	mu       sync.RWMutex
}

func NewCommandBus() *simpleCommandBus {
	return &simpleCommandBus{
		handlers: make(map[string]CommandHandler),
	}
}

func (b *simpleCommandBus) RegisterHandler(cmd Command, handler CommandHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.handlers[TypeName(cmd)]
	if exists {
		panic("handler already registered")
	}
	b.handlers[TypeName(cmd)] = handler
}

func (b *simpleCommandBus) Send(cmd Command) error {
	b.mu.RLock()
	h, ok := b.handlers[TypeName(cmd)]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no handler for command %s", TypeName(cmd))
	}
	return h.Handle(cmd)
}

func (b *simpleCommandBus) MustSend(cmd Command) {
	err := b.Send(cmd)
	if err != nil {
		panic(err)
	}
}
