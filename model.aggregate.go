package eventsource

import "context"

type Aggregate interface {
	ID() AggregateID
	Type() AggregateType
	Version() AggregateVersion
	Changes() []Event
	StackChange(change Event)
	StackSnapshot(snapshot *Snapshot)
	StackedSnapshots() []*Snapshot
	SnapshotsWithFrequency(frequency int) []*Snapshot
	SetVersion(version AggregateVersion)
	IncrementVersion()
	PrepareForLoading()
	ParseEvents(context.Context, ...EventReadModel) []Event
}

type BaseAggregate struct {
	id        AggregateID
	t         AggregateType
	v         AggregateVersion
	changes   []Event
	snapshots []*Snapshot
}

func InitAggregate(id string, t AggregateType) *BaseAggregate {
	return &BaseAggregate{
		id:        AggregateID(id),
		t:         t,
		v:         0,
		changes:   make([]Event, 0),
		snapshots: make([]*Snapshot, 0),
	}
}

func (a *BaseAggregate) PrepareForLoading() {
	a.v = 0
	a.changes = make([]Event, 0)
	a.snapshots = make([]*Snapshot, 0)
}

func (a *BaseAggregate) Type() AggregateType {
	return a.t
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
