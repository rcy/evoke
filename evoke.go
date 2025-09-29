package evoke

import (
	"reflect"

	"github.com/google/uuid"
)

// Return a string corresponding to this type
func TypeName(evt any) string {
	t := reflect.TypeOf(evt)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

type Command interface {
	// Return the aggregate ID from the Command
	AggregateID() uuid.UUID
}

type CommandHandler interface {
	Handle(Command) error
}

type CommandSender interface {
	Send(cmd Command) error
	MustSend(cmd Command)
}

type EventStore interface {
	Record(aggregateID uuid.UUID, evs []Event) error
	MustRecord(aggregateID uuid.UUID, evs []Event)
	LoadStream(aggregateID uuid.UUID) ([]RecordedEvent, error)
	ReplayFrom(seq int64, handler RecordedEventHandlerFunc) error
	RegisterPublisher(publisher RecordedEventPublisher)
}

// Events are whatever you want them to be
type Event interface{}

type EventHandler interface {
	Handle(Event, bool) error
}

type RecordedEventHandlerFunc func(rec RecordedEvent, replay bool) error

type RecordedEventPublisher interface {
	Publish(rec RecordedEvent, replay bool) error
}

type EventBus interface {
	Subscribe(eventType string, handler EventHandler)
	RecordedEventPublisher
}

type RecordedEvent struct {
	Sequence    int64
	RecordedAt  int64
	AggregateID uuid.UUID
	Event       Event
	EventType   string
}

type Aggregate interface {
	HandleCommand(cmd Command) ([]Event, error)
	Apply(e Event) error
}
