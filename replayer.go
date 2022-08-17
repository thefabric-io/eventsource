package eventsource

type Replayer interface {
	Replay(id AggregateID, snapshot *Snapshot, ee ...Event) Aggregator
}
