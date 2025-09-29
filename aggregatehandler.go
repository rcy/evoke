package evoke

import (
	"fmt"

	"github.com/google/uuid"
)

type AggregateHandler struct {
	aggregateFactory func(id uuid.UUID) Aggregate
	store            EventStore
}

func NewAggregateHandler(store EventStore, factory func(id uuid.UUID) Aggregate) *AggregateHandler {
	return &AggregateHandler{
		aggregateFactory: factory,
		store:            store,
	}
}

func NewAggregateHandler2(store EventStore, factory func(id uuid.UUID) Aggregate) *AggregateHandler {
	return &AggregateHandler{
		aggregateFactory: factory,
		store:            store,
	}
}

func (h *AggregateHandler) Handle(cmd Command) error {
	aggID := cmd.AggregateID()

	// rehydrate aggregate from store
	agg := h.aggregateFactory(aggID)
	recs, err := h.store.LoadStream(aggID)
	if err != nil {
		return fmt.Errorf("LoadStream(%s): %w", aggID, err)
	}

	for _, rec := range recs {
		err := agg.Apply(rec.Event)
		if err != nil {
			return fmt.Errorf("Apply(%T): %w", rec.Event, err)
		}
	}

	// handle command
	newEvents, err := agg.HandleCommand(cmd)
	if err != nil {
		return fmt.Errorf("%T.HandleCommand(%T): error: %w", agg, cmd, err)
	}

	// persist
	err = h.store.Record(aggID, newEvents)
	if err != nil {
		return err
	}

	return nil
}
