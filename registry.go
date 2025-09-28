package evoke

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func RegisterEvent[T Event](er EventRegisterer, ctor T) {
	er.registerEvent(TypeName(ctor), func() Event {
		return ctor
	})
}

type EventRegisterer interface {
	registerEvent(eventType string, ctor func() Event)
	UnmarshalEvent(eventType string, data []byte) (Event, error)
}

type EventRegistry struct {
	registry map[string]func() Event
}

func (er *EventRegistry) registerEvent(eventType string, ctor func() Event) {
	if er.registry == nil {
		er.registry = make(map[string]func() Event)
	}
	er.registry[eventType] = ctor
}

func (er *EventRegistry) UnmarshalEvent(eventType string, data []byte) (Event, error) {
	ctor, ok := er.registry[eventType]
	if !ok {
		return nil, fmt.Errorf("event not registered %q (hint call evoke.RegisterEvent(...)", eventType)
	}
	e := ctor()
	if err := json.Unmarshal(data, e); err != nil {
		return nil, err
	}

	// return underlying values not pointers
	v := reflect.ValueOf(e)
	if v.Kind() == reflect.Ptr {
		return v.Elem().Interface().(Event), nil
	}

	return e, nil
}
