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

type TxAccessMode string

func (t TxAccessMode) String() string {
	return string(t)
}

const (
	ReadWrite TxAccessMode = "read write"
	ReadOnly  TxAccessMode = "read only"
)

type TxIsoLevel string

func (t TxIsoLevel) String() string {
	return string(t)
}

const (
	Serializable    TxIsoLevel = "serializable"
	RepeatableRead  TxIsoLevel = "repeatable read"
	ReadCommitted   TxIsoLevel = "read committed"
	ReadUncommitted TxIsoLevel = "read uncommitted"
)

type TxDeferrableMode string

func (t TxDeferrableMode) String() string {
	return string(t)
}

const (
	Deferrable    TxDeferrableMode = "deferrable"
	NotDeferrable TxDeferrableMode = "not deferrable"
)

type SaveOptions struct {
	WithSnapshot          bool
	WithSnapshotFrequency int
	MustSendToOutbox      bool
}

type EventStore interface {
	Save(ctx context.Context, tx Transaction, a Aggregator, opts SaveOptions) error
	Load(ctx context.Context, tx Transaction, id AggregateID, parser EventParser, replayer Replayer) (Aggregator, error)
	BeginTransaction(ctx context.Context, opts BeginTransactionOptions) (Transaction, error)
	Ping(ctx context.Context) error
}

type BeginTransactionOptions struct {
	AccessMode     TxAccessMode
	IsolationLevel TxIsoLevel
	DeferrableMode TxDeferrableMode
}

func DefaultTransactionOptions() BeginTransactionOptions {
	return BeginTransactionOptions{
		AccessMode:     ReadWrite,
		IsolationLevel: Serializable,
		DeferrableMode: NotDeferrable,
	}
}

type Transaction interface {
	Commit() error
	Rollback() error
}
