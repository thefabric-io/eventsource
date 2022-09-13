package eventsource

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"errors"
)

var (
	ErrAggregateReferenceIsRequired = errors.New("aggregate reference is required and cannot be nil")
	ErrAggregateIDIsRequired        = errors.New("aggregate id is required")
	ErrAggregateTypeIsRequired      = errors.New("aggregate type is required")
)

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

func NewBaseAggregate(id string, t AggregateType) (*BaseAggregate, error) {
	aId := AggregateID(id)
	if aId.IsZero() {
		return nil, ErrAggregateIDIsRequired
	}

	if t.IsZero() {
		return nil, ErrAggregateTypeIsRequired
	}

	res := BaseAggregate{
		id:        AggregateID(id),
		t:         t,
		changes:   make([]Event, 0),
		snapshots: make([]*Snapshot, 0),
	}

	return &res, nil
}

func InitBaseAggregate(id AggregateID, a Aggregator) (*BaseAggregate, error) {
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
