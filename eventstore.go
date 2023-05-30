package eventsource

import (
	"context"
	"errors"
)

var (
	ErrNoEventsToStore       = errors.New("no events to store")
	ErrNoSnapshotFound       = errors.New("no snapshot found")
	ErrTransactionIsRequired = errors.New("transaction is required")
	ErrAggregateDoNotExist   = errors.New("aggregate do not exist")
)

func ErrIsSnapshotNotFound(err error) bool {
	return errors.Is(err, ErrNoSnapshotFound)
}

type SaveOption func(*SaveOptions)

func WithSnapshot(frequency int) SaveOption {
	return func(opt *SaveOptions) {
		opt.WithSnapshot = frequency != 0
		opt.WithSnapshotFrequency = frequency
	}
}

func NewSaveOptions(opts ...SaveOption) *SaveOptions {
	const (
		defaultWithSnapshot          = true
		defaultWithSnapshotFrequency = 10
	)

	result := &SaveOptions{
		WithSnapshot:          defaultWithSnapshot,
		WithSnapshotFrequency: defaultWithSnapshotFrequency,
	}

	for _, opt := range opts {
		opt(result)
	}

	return result
}

type SaveOptions struct {
	WithSnapshot          bool
	WithSnapshotFrequency int
}

type EventStore interface {
	Save(ctx context.Context, tx Transaction, a Aggregate, opts ...SaveOption) error
	Load(ctx context.Context, tx Transaction, a Aggregate) (Aggregate, error)
	EventsHistory(ctx context.Context, tx Transaction, aggregateID, aggregateType string, fromVersion int, limit int) ([]EventReadModel, error)
}

type Transaction interface {
	Commit() error
	Rollback() error
}
