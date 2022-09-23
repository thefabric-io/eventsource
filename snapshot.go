package eventsource

import (
	"log"
	"time"
)

func FromSnapshot(snapshot *Snapshot, a Aggregate) {
	if snapshot != nil {
		if err := UnmarshalES(snapshot.Data, a); err != nil {
			log.Printf("could not unserialize snapshot of aggregate '%s'", snapshot.AggregateID)
			return
		}

		a.SetVersion(snapshot.AggregateVersion)
	}
}

func NewSnapshot(a Aggregate) (*Snapshot, error) {
	b, err := MarshalES(a)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		AggregateID:      a.ID(),
		AggregateType:    a.Type(),
		AggregateVersion: a.Version(),
		TakenAt:          time.Now(),
		Data:             b,
	}, nil
}

type Snapshot struct {
	AggregateID      AggregateID
	AggregateType    AggregateType
	AggregateVersion AggregateVersion
	TakenAt          time.Time
	Data             []byte
}
