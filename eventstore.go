package eventsource

import (
	"context"
	"errors"
)

var (
	ErrNoEventsToStore       = errors.New("no events to store")
	ErrEventParserIsRequired = errors.New("event parser is required")
	ErrNoSnapshotFound       = errors.New("no snapshot found")
	ErrTransactionIsRequired = errors.New("transaction is required")
	ErrAggregateDoNotExist   = errors.New("aggregate do not exist")
)

func ErrIsSnapshotNotFound(err error) bool {
	return errors.Is(err, ErrNoSnapshotFound)
}

type SaveOptions struct {
	WithSnapshot          bool
	WithSnapshotFrequency int
	MustSendToOutbox      bool
}

type EventStore interface {
	Save(ctx context.Context, tx Transaction, a Aggregator, opts SaveOptions) error
	Load(ctx context.Context, tx Transaction, id AggregateID, parser EventParser, replayer Replayer) (Aggregator, error)
}

type Transaction interface {
	Commit() error
	Rollback() error
}