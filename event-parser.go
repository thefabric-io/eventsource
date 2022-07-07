package eventsource

type EventParser interface {
	Replay(id AggregateID, snapshot *Snapshot, ee ...Event) Aggregator
	ParseEvent(model EventReadModel) Event
}
