package eventsource

import (
	"time"
)

type Snapshot struct {
	AggregateID      AggregateID
	AggregateType    AggregateType
	AggregateVersion AggregateVersion
	TakenAt          time.Time
	Data             []byte
}

func (s Snapshot) NextVersion() AggregateVersion {
	return s.AggregateVersion + 1
}
