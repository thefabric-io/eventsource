package postgres

import (
	"database/sql"
	"encoding/json"

	"github.com/thefabric-io/eventsource"
)

func FromSnapshot(s eventsource.Snapshot) *Snapshot {
	return &Snapshot{
		AggregateID: sql.NullString{
			String: s.AggregateID.String(),
			Valid:  !s.AggregateID.IsZero(),
		},
		AggregateType: sql.NullString{
			String: s.AggregateType.String(),
			Valid:  !s.AggregateType.IsZero(),
		},
		AggregateVersion: sql.NullInt64{
			Int64: s.AggregateVersion.Int64(),
			Valid: !s.AggregateVersion.IsZero(),
		},
		TakenAt: sql.NullTime{
			Time:  s.TakenAt,
			Valid: !s.TakenAt.IsZero(),
		},
		Data: s.Data,
	}
}

type Snapshot struct {
	AggregateID      sql.NullString
	AggregateType    sql.NullString
	AggregateVersion sql.NullInt64
	TakenAt          sql.NullTime
	Data             json.RawMessage
}

func (s *Snapshot) ToSnapshot() *eventsource.Snapshot {
	return &eventsource.Snapshot{
		AggregateID:      eventsource.AggregateID(s.AggregateID.String),
		AggregateType:    eventsource.AggregateType(s.AggregateType.String),
		AggregateVersion: eventsource.AggregateVersion(s.AggregateVersion.Int64),
		TakenAt:          s.TakenAt.Time,
		Data:             s.Data,
	}
}
