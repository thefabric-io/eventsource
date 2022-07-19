package eventsource

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/thefabric-io/errors"
)

var ErrAggregateReferenceIsRequired = errors.New("aggregate reference is required and cannot be nil", "ErrAggregateReferenceIsRequired")

type Aggregator interface {
	Snapshoter
	ID() AggregateID
	Type() AggregateType
	Version() AggregateVersion
	Changes() []Event
	StackChange(change Event)
	StackSnapshot(snapshot *Snapshot)
	StackedSnapshots() []*Snapshot
	SnapshotsWithFrequency(frequency int) []*Snapshot
	IncrementVersion()
	Data() any
}

type BaseAggregate struct {
	id        AggregateID
	t         AggregateType
	v         AggregateVersion
	changes   []Event
	snapshots []*Snapshot
	data      any
}

func NewAggregateModel(p IdentityPrefix, a Aggregator) (*BaseAggregate, error) {
	res, err := InitAggregateModel(NewID(p), a)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func InitAggregateModel(id AggregateID, a Aggregator) (*BaseAggregate, error) {
	if a == nil {
		return nil, ErrAggregateReferenceIsRequired
	}

	res := BaseAggregate{
		id:        id,
		t:         a.Type(),
		v:         0,
		changes:   make([]Event, 0),
		snapshots: make([]*Snapshot, 0),
		data:      a.Data(),
	}

	return &res, nil
}

func Raise(aggregate Aggregator, change Event) {
	if aggregate == nil || change == nil {
		return
	}

	aggregate.StackChange(change)

	change.SetVersion(aggregate.Version() + 1)

	On(aggregate, change)
}

func On(a Aggregator, event Event) {
	if a == nil || event == nil {
		return
	}

	if a.Version() < event.AggregateVersion() {
		event.ApplyTo(a)

		log.Printf("applied event `%s` with id `%s` on aggregate with id `%s`", event.Type(), event.ID(), event.AggregateID())

		a.IncrementVersion()

		a.StackSnapshot(a.ForceSnapshot())
	}
}

func (a *BaseAggregate) ForceSnapshot() *Snapshot {
	s, err := a.Snapshot()
	if err != nil {
		log.Printf("error snapshoting aggregate with id `%s`", a.ID())
	}

	return s
}

func (a *BaseAggregate) Snapshot() (*Snapshot, error) {
	var err error

	s := Snapshot{
		AggregateID:      a.id,
		AggregateType:    a.t,
		AggregateVersion: a.v,
		TakenAt:          time.Now(),
		Data:             nil,
	}

	s.Data, err = json.Marshal(a.data)
	if err != nil {
		err = fmt.Errorf("error marshaling aggregate with id '%s' and type '%s': %w", a.id, a.t, err)

		return &s, err
	}

	return &s, nil
}

func (a *BaseAggregate) FromSnapshot(snapshot *Snapshot) {
	if snapshot != nil {
		if err := json.Unmarshal(snapshot.Data, a.data); err != nil {
			log.Printf("error in replay from snapshot for aggregate with id '%s'", snapshot.AggregateID)
		}

		a.SetVersion(snapshot.AggregateVersion)
	}
}

func (a *BaseAggregate) IncrementVersion() {
	a.v = a.v.NextVersion()
}

func (a *BaseAggregate) SetVersion(version AggregateVersion) {
	a.v = version
}

func (a *BaseAggregate) StackChange(change Event) {
	a.changes = append(a.changes, change)
}

func (a *BaseAggregate) StackSnapshot(snapshot *Snapshot) {
	a.snapshots = append(a.snapshots, snapshot)
}

func (a *BaseAggregate) StackedSnapshots() []*Snapshot {
	return a.snapshots
}

func (a *BaseAggregate) SnapshotsWithFrequency(frequency int) []*Snapshot {
	results := make([]*Snapshot, 0)

	if frequency == 0 {
		return results
	}

	for _, snapshot := range a.StackedSnapshots() {
		if int(snapshot.AggregateVersion)%frequency == 0 {
			results = append(results, snapshot)
		}
	}

	return results
}

func (a *BaseAggregate) ID() AggregateID {
	return a.id
}

func (a *BaseAggregate) Version() AggregateVersion {
	return a.v
}

func (a *BaseAggregate) Changes() []Event {
	return a.changes
}
