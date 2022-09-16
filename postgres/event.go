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
	OccurredAt       sql.NullTime
	RegisteredAt     sql.NullTime
	AggregateID      sql.NullString
	AggregateType    sql.NullString
	AggregateVersion sql.NullInt64
	Data             json.RawMessage
	Metadata         json.RawMessage
}

func FromEvent(event eventsource.Event) (*Event, error) {
	data, err := event.Serialize()
	if err != nil {
		return nil, err
	}

	metadata, err := event.Metadata().Serialize()
	if err != nil {
		return nil, err
	}

	return &Event{
		ID:               sql.NullString{String: event.ID().String(), Valid: !event.ID().IsZero()},
		Type:             sql.NullString{String: event.Type().String(), Valid: !event.Type().IsZero()},
		OccurredAt:       sql.NullTime{Time: event.OccurredAt(), Valid: !event.OccurredAt().IsZero()},
		RegisteredAt:     sql.NullTime{Time: time.Now(), Valid: !event.OccurredAt().IsZero()},
		AggregateID:      sql.NullString{String: event.AggregateID().String(), Valid: !event.AggregateID().IsZero()},
		AggregateType:    sql.NullString{String: event.AggregateType().String(), Valid: !event.AggregateType().IsZero()},
		AggregateVersion: sql.NullInt64{Int64: event.AggregateVersion().Int64(), Valid: !event.AggregateVersion().IsZero()},
		Data:             data,
		Metadata:         metadata,
	}, nil
}

func (e *Event) ToReadModel() eventsource.EventReadModel {
	var metadata map[string]interface{}
	_ = json.Unmarshal(e.Metadata, &metadata)

	return eventsource.EventReadModel{
		ID:               eventsource.EventID(e.ID.String),
		Type:             eventsource.EventType(e.Type.String),
		OccurredAt:       e.OccurredAt.Time,
		AggregateID:      eventsource.AggregateID(e.AggregateID.String),
		AggregateType:    eventsource.AggregateType(e.AggregateType.String),
		AggregateVersion: eventsource.AggregateVersion(e.AggregateVersion.Int64),
		Metadata:         metadata,
		Data:             e.Data,
	}
}
