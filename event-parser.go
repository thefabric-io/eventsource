package eventsource

type EventParser interface {
	ParseEvent(model EventReadModel) Event
}
