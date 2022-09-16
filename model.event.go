package eventsource

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

func Sort(ee []Event) {
	sort.Slice(ee, func(i, j int) bool {
		return ee[i].AggregateVersion() < ee[j].AggregateVersion()
	})
}

type Event interface {
	fmt.Stringer
	DataSerializer
	ApplyTo(aggregate Aggregator)       // ApplyTo applies the event to the aggregate
	ID() EventID                        // ID returns the id of the event.
	Type() EventType                    // Type returns the type of the event.
	OccurredAt() time.Time              // OccurredAt of when the event was created.
	AggregateID() AggregateID           // AggregateID is the id of the aggregate that the event belongs to.
	AggregateType() AggregateType       // AggregateType is the type of the aggregate that the event can be applied to.
	AggregateVersion() AggregateVersion // AggregateVersion is the version of the aggregate after the event has been applied.
	SetVersion(AggregateVersion)        // SetVersion sets the aggregate version of the event
	Metadata() Metadata                 // Metadata is app-specific metadata such as request AggregateID, originating user etc.
}

func NewMetadata() Metadata {
	return Metadata{}
}

type Metadata map[string]interface{}

func (m Metadata) Add(key string, value interface{}) Metadata {
	m[key] = value

	return m
}

func (m Metadata) Serialize() ([]byte, error) {
	return json.Marshal(&m)
}

func (m *Metadata) Deserialize(b []byte) error {
	return json.Unmarshal(b, m)
}

type EventType string

func (t EventType) String() string {
	return string(t)
}

func (t EventType) IsZero() bool {
	return len(strings.TrimSpace(t.String())) == 0
}

func NewBaseEvent(from Aggregator, metadata Metadata) *BaseEvent {
	return initBaseEvent(NewEventID(), time.Now(), from.ID(), from.Type(), from.Version(), metadata)
}

func initBaseEvent(id EventID, occurredAt time.Time, aggregateID AggregateID, aggregateType AggregateType, version AggregateVersion, metadata Metadata) *BaseEvent {
	if metadata == nil {
		metadata = make(Metadata, 0)
	}

	return &BaseEvent{
		id:               id,
		occurredAt:       occurredAt,
		aggregateID:      aggregateID,
		aggregateType:    aggregateType,
		aggregateVersion: version,
		metadata:         metadata,
	}
}

type BaseEvent struct {
	id               EventID
	occurredAt       time.Time
	aggregateID      AggregateID
	aggregateType    AggregateType
	aggregateVersion AggregateVersion
	metadata         map[string]interface{}
}

func (e *BaseEvent) ID() EventID {
	return e.id
}

func (e *BaseEvent) OccurredAt() time.Time {
	return e.occurredAt
}

func (e *BaseEvent) AggregateID() AggregateID {
	return e.aggregateID
}

func (e *BaseEvent) AggregateType() AggregateType {
	return e.aggregateType
}

func (e *BaseEvent) AggregateVersion() AggregateVersion {
	return e.aggregateVersion
}

func (e *BaseEvent) Metadata() Metadata {
	return e.metadata
}

func (e *BaseEvent) String() string {
	return fmt.Sprintf("event '%s' occurred on aggregate '%s' (v%d => v%d) with id '%s'", e.ID(), e.AggregateType(), e.aggregateVersion-1, e.aggregateVersion, e.AggregateID())
}

func (e *BaseEvent) SetVersion(version AggregateVersion) {
	e.aggregateVersion = version
}

type EventReadModel struct {
	ID               EventID                `json:"id"`
	Type             EventType              `json:"type"`
	OccurredAt       time.Time              `json:"occurred_at"`
	AggregateID      AggregateID            `json:"aggregate_id"`
	AggregateType    AggregateType          `json:"aggregate_type"`
	AggregateVersion AggregateVersion       `json:"aggregate_version"`
	Metadata         map[string]interface{} `json:"metadata"`
	Data             json.RawMessage        `json:"data"`
}

func (r *EventReadModel) InitBaseEvent() *BaseEvent {
	return initBaseEvent(
		r.ID,
		r.OccurredAt,
		r.AggregateID,
		r.AggregateType,
		r.AggregateVersion,
		r.Metadata,
	)
}
