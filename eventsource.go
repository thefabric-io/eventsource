package eventsource

import "log"

func Raise(aggregate Aggregator, change Event) {
	if aggregate == nil || change == nil {
		return
	}

	aggregate.StackChange(change)

	change.SetVersion(aggregate.Version() + 1)

	On(aggregate, change, true)
}

func On(a Aggregator, event Event, new bool) {
	if a == nil || event == nil {
		return
	}

	if a.Version() < event.AggregateVersion() {
		event.ApplyTo(a)

		action := "replayed"
		if new {
			action = "applied"
		}

		log.Printf("%s event `%s` with id `%s` on aggregate with id `%s`", action, event.Type(), event.ID(), event.AggregateID())

		a.IncrementVersion()

		a.StackSnapshot(a.ForceSnapshot())
	}
}
