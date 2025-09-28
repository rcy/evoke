package evoke

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type simpleStore struct {
	mu           sync.Mutex
	events       []RecordedEvent
	streams      map[uuid.UUID][]RecordedEvent
	nextSequence int64
	publishers   []RecordedEventPublisher
}

func NewSimpleStore(bus EventBus) *simpleStore {
	return &simpleStore{
		events:       make([]RecordedEvent, 0),
		streams:      make(map[uuid.UUID][]RecordedEvent),
		nextSequence: 1,
		publishers:   []RecordedEventPublisher{},
	}
}

func (s *simpleStore) RegisterPublisher(publisher RecordedEventPublisher) {
	s.publishers = append(s.publishers, publisher)
}

// Return all events, not really a long term method here
func (s *simpleStore) DebugEvents() ([]RecordedEvent, error) {
	return s.events, nil
}

func (s *simpleStore) appendEvents(aggregateID uuid.UUID, evs []Event) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(evs) == 0 {
		return nil, errors.New("no events to append")
	}

	out := make([]RecordedEvent, 0, len(evs))
	for _, e := range evs {
		rec := RecordedEvent{
			Sequence:    s.nextSequence,
			AggregateID: aggregateID,
			Event:       e,
			//Timestamp:   time.Now(),
		}
		s.nextSequence++

		s.events = append(s.events, rec)
		s.streams[aggregateID] = append(s.streams[aggregateID], rec)

		out = append(out, rec)
	}
	return out, nil
}

func (s *simpleStore) Record(aggregateID uuid.UUID, evs []Event) error {
	recs, err := s.appendEvents(aggregateID, evs)
	if err != nil {
		return err
	}

	for _, p := range s.publishers {
		for _, rec := range recs {
			err := p.Publish(rec, false)
			if err != nil {
				return fmt.Errorf("publish: %w", err)
			}
		}
	}

	return nil
}

func (s *simpleStore) LoadStream(aggregateID uuid.UUID) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[aggregateID]
	cpy := make([]RecordedEvent, len(stream))
	copy(cpy, stream)
	return cpy, nil
}

func (s *simpleStore) TailFrom(seq int64, callback func(RecordedEvent) error) error {
	s.mu.Lock()
	start := seq
	existing := make([]RecordedEvent, 0)
	for _, e := range s.events {
		if e.Sequence >= start {
			existing = append(existing, e)
		}
	}
	s.mu.Unlock()

	for _, e := range existing {
		if err := callback(e); err != nil {
			return err
		}
	}

	return nil
}
