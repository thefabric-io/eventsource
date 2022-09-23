package eventsource

import "context"

type EventParser interface {
	ParseEvents(ctx context.Context, id AggregateID, model ...EventReadModel) ([]Event, Aggregate)
}
