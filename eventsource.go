package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log"

	jsoniter "github.com/json-iterator/go"
)

var (
	ErrAssertionFailed = errors.New("assertion failed")
)

func AssertAndGet[H Aggregate, W Aggregate](have H, want W) (W, error) {
	r, ok := Aggregate(have).(W)
	if !ok {
		return r, fmt.Errorf("%w: received type '%s'", ErrAssertionFailed, want.Type())
	}

	return r, nil
}

func Raise(ctx context.Context, aggregate Aggregate, changes ...Event) {
	if aggregate == nil || len(changes) == 0 {
		return
	}

	for _, e := range changes {
		if e == nil {
			continue
		}

		aggregate.StackChange(e)

		e.SetVersion(aggregate.Version() + 1)

		On(ctx, aggregate, e, true)
	}
}

func On(ctx context.Context, a Aggregate, event Event, new bool) {
	if a == nil || event == nil {
		return
	}

	if a.Version() < event.AggregateVersion() {
		event.ApplyTo(ctx, a)

		action := "replayed"
		if new {
			action = "raised"
		}

		log.Printf("%s event `%s` with id `%s` on aggregate with id `%s`", action, event.Type(), event.ID(), event.AggregateID())

		a.IncrementVersion()

		snap, err := NewSnapshot(a)
		if err != nil {
			log.Println(err)
		}

		a.StackSnapshot(snap)
	}
}

func Replay(ctx context.Context, id AggregateID, a Aggregate, snapshot *Snapshot, ee ...Event) (Aggregate, error) {
	FromSnapshot(snapshot, a)

	Sort(ee)

	for _, e := range ee {
		On(ctx, a, e, false)
	}

	return a, nil
}

func MarshalES(object any) ([]byte, error) {
	if s, implements := object.(Marshaler); implements {
		return s.MarshalES()
	}

	jsonc := jsoniter.Config{TagKey: "es"}.Froze()

	bytes, err := jsonc.Marshal(object)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func UnmarshalES(b []byte, object any) error {
	if s, implements := object.(Marshaler); implements {
		return s.UnmarshalES(b, object)
	}

	jsonc := jsoniter.Config{TagKey: "es"}.Froze()

	if err := jsonc.Unmarshal(b, object); err != nil {
		return err
	}

	return nil
}
