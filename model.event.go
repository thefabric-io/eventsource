package eventsource

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

func Sort(ee []Event) {
	sort.Slice(ee, func(i, j int) bool {
		return ee[i].AggregateVersion() < ee[j].AggregateVersion()
	})
}

type Event interface {
	fmt.Stringer
	ApplyTo(aggregate Aggregator)       // ApplyTo applies the event to the aggregate
	ID() EventID                        // ID returns the id of the event.
	Type() EventType                    // Type returns the type of the event.
	OccurredAt() time.Time              // OccurredAt of when the event was created.
	AggregateID() AggregateID           // AggregateID is the id of the aggregate that the event belongs to.
	AggregateType() AggregateType       // AggregateType is the type of the aggregate that the event can be applied to.
	AggregateVersion() AggregateVersion // AggregateVersion is the version of the aggregate after the event has been applied.
	SetVersion(AggregateVersion)        // SetVersion sets the aggregate version of the event
	Data() json.RawMessage              // Data returns the raw json format of the event
	Metadata() json.RawMessage          // Metadata is app-specific metadata such as request AggregateID, originating user etc.
}

type Metadata = map[string]interface{}

type EventType string

func (t EventType) String() string {
	return string(t)
}

func (t EventType) IsZero() bool {
	return len(strings.TrimSpace(t.String())) == 0
}

func NewEventModel(from Aggregator, metadata Metadata) *BaseEvent {
	return initEventModel(EventID(uuid.New().String()), time.Now(), from.ID(), from.Type(), from.Version(), metadata)
}

func initEventModel(id EventID, occurredAt time.Time, aggregateID AggregateID, aggregateType AggregateType, version AggregateVersion, metadata Metadata) *BaseEvent {
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

func (e BaseEvent) ID() EventID {
	return e.id
}

func (e BaseEvent) OccurredAt() time.Time {
	return e.occurredAt
}

func (e BaseEvent) AggregateID() AggregateID {
	return e.aggregateID
}

func (e BaseEvent) AggregateType() AggregateType {
	return e.aggregateType
}

func (e BaseEvent) AggregateVersion() AggregateVersion {
	return e.aggregateVersion
}

func (e BaseEvent) Metadata() json.RawMessage {
	b, err := json.Marshal(e.metadata)
	if err != nil {
		return json.RawMessage{}
	}

	return b
}

func (e BaseEvent) String() string {
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
	return initEventModel(
		r.ID,
		r.OccurredAt,
		r.AggregateID,
		r.AggregateType,
		r.AggregateVersion,
		r.Metadata,
	)
}
