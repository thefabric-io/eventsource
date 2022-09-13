package eventsource

import (
	"context"

	"github.com/thefabric-io/errors"
)

var (
	ErrNoEventsToStore           = errors.New("no events to store", "ErrNoEventsToStore")
	ErrAggregateParserIsRequired = errors.New("aggregate parser is required", "ErrAggregateParserIsRequired")
	ErrNoSnapshotFound           = errors.New("no snapshot found", "ErrNoSnapshotFound")
	ErrTransactionIsRequired     = errors.New("transaction is required", "ErrTransactionIsRequired")
	ErrAggregateDoNotExist       = errors.New("aggregate do not exist", "ErrAggregateDoNotExist")
)

func ErrIsSnapshotNotFound(err error) bool {
	return errors.IsWithStrategy(err, ErrNoSnapshotFound, errors.CompareStrictStrategy())
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
