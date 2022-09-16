package eventsource

import (
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	ErrAggregateReferenceIsRequired = errors.New("aggregate reference is required and cannot be nil")
	ErrAggregateIDIsRequired        = errors.New("aggregate id is required")
)

type Aggregator interface {
	Snapshoter
	DataSerializer
	SetBaseAggregate(*BaseAggregate) error
	ID() AggregateID
	Type() AggregateType
	Version() AggregateVersion
	Changes() []Event
	StackChange(change Event)
	StackSnapshot(snapshot *Snapshot)
	StackedSnapshots() []*Snapshot
	SnapshotsWithFrequency(frequency int) []*Snapshot
	IncrementVersion()
}

type Snapshoter interface {
	Snapshot() (*Snapshot, error)
	ForceSnapshot() *Snapshot
}

type DataSerializer interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type BaseAggregate struct {
	id        AggregateID
	t         AggregateType
	v         AggregateVersion
	changes   []Event
	snapshots []*Snapshot
	aggregate Aggregator
}

func InitBaseAggregate(id AggregateID, ref Aggregator) (*BaseAggregate, error) {
	if id.IsZero() {
		return nil, ErrAggregateIDIsRequired
	}

	if ref == nil {
		return nil, ErrAggregateReferenceIsRequired
	}

	res := BaseAggregate{
		id:        id,
		t:         ref.Type(),
		v:         0,
		changes:   make([]Event, 0),
		snapshots: make([]*Snapshot, 0),
		aggregate: ref,
	}

	return &res, nil
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

	s.Data, err = a.aggregate.Serialize()
	if err != nil {
		err = fmt.Errorf("error marshaling aggregate with id '%s' and type '%s': %w", a.id, a.t, err)

		return &s, err
	}

	return &s, nil
}

func (a *BaseAggregate) FromSnapshot(snapshot *Snapshot) {
	if snapshot != nil {

		if err := a.aggregate.Deserialize(snapshot.Data); err != nil {
			log.Printf("error unmarshaling from snapshot for aggregate with id '%s'", snapshot.AggregateID)
		}

		a.SetVersion(snapshot.AggregateVersion)
	}
}

func (a *BaseAggregate) IncrementVersion() {
	a.v = a.v.Next()
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
