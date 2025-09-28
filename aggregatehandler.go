package evoke

import "github.com/google/uuid"

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
	if events, err := h.store.LoadStream(aggID); err == nil {
		for _, re := range events {
			agg.Apply(re.Event)
		}
	}

	// handle command
	newEvents, err := agg.HandleCommand(cmd)
	if err != nil {
		return err
	}

	// persist
	err = h.store.Record(aggID, newEvents)
	if err != nil {
		return err
	}

	return nil
}
