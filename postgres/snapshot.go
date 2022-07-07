package postgres

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/thefabric-io/eventsource"
)

type Snapshot struct {
	AggregateID      sql.NullString
	AggregateType    sql.NullString
	AggregateVersion sql.NullInt64
	TakenAt          time.Time
	Data             json.RawMessage
}

func (s *Snapshot) ToSnapshot() *eventsource.Snapshot {
	return &eventsource.Snapshot{
		AggregateID:      eventsource.AggregateID(s.AggregateID.String),
		AggregateType:    eventsource.AggregateType(s.AggregateType.String),
		AggregateVersion: eventsource.AggregateVersion(s.AggregateVersion.Int64),
		TakenAt:          s.TakenAt,
		Data:             s.Data,
	}
}
