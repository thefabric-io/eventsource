package postgres

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/thefabric-io/eventsource"
)

type Event struct {
	ID               sql.NullString
	Type             sql.NullString
	OccurredAt       time.Time
	RegisteredAt     time.Time
	AggregateID      sql.NullString
	AggregateType    sql.NullString
	AggregateVersion sql.NullInt64
	Data             json.RawMessage
	Metadata         json.RawMessage
}

func (e *Event) ToReadModel() eventsource.EventReadModel {
	var metadata map[string]interface{}
	_ = json.Unmarshal(e.Metadata, &metadata)

	return eventsource.EventReadModel{
		ID:               eventsource.EventID(e.ID.String),
		Type:             eventsource.EventType(e.Type.String),
		OccurredAt:       e.OccurredAt,
		AggregateID:      eventsource.AggregateID(e.AggregateID.String),
		AggregateType:    eventsource.AggregateType(e.AggregateType.String),
		AggregateVersion: eventsource.AggregateVersion(e.AggregateVersion.Int64),
		Metadata:         metadata,
		Data:             e.Data,
	}
}
