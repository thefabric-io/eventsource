package eventsource

import (
	"encoding/json"
	"time"
)

type Snapshoter interface {
	Snapshot() (*Snapshot, error)
	ForceSnapshot() *Snapshot
}

type Snapshot struct {
	AggregateID      AggregateID
	AggregateType    AggregateType
	AggregateVersion AggregateVersion
	TakenAt          time.Time
	Data             json.RawMessage
}

func (s Snapshot) NextVersion() AggregateVersion {
	return s.AggregateVersion + 1
}
