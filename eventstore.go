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

func SendToOutbox(b bool) SaveOption {
	return func(opt *SaveOptions) {
		opt.MustSendToOutbox = b
	}
}

func NewSaveOptions(opts ...SaveOption) *SaveOptions {
	const (
		defaultWithSnapshot          = true
		defaultWithSnapshotFrequency = 10
		defaultMustSendToOutbox      = true
	)

	result := &SaveOptions{
		WithSnapshot:          defaultWithSnapshot,
		WithSnapshotFrequency: defaultWithSnapshotFrequency,
		MustSendToOutbox:      defaultMustSendToOutbox,
	}

	for _, opt := range opts {
		opt(result)
	}

	return result
}

type SaveOptions struct {
	WithSnapshot          bool
	WithSnapshotFrequency int
	MustSendToOutbox      bool
}

type EventStore interface {
	Save(ctx context.Context, tx Transaction, a Aggregate, opts ...SaveOption) error
	Load(ctx context.Context, tx Transaction, a Aggregate) (Aggregate, error)
}

type Transaction interface {
	Commit() error
	Rollback() error
}
