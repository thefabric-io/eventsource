package eventsource

import (
	"fmt"
	"strings"

	"github.com/segmentio/ksuid"
)

type AggregateID string

func (id AggregateID) String() string {
	return string(id)
}

func (id AggregateID) IsZero() bool {
	return len(strings.TrimSpace(string(id))) == 0
}

func NewEventID() EventID {
	return EventID(fmt.Sprintf("evt_%s", ksuid.New().String()))
}

type EventID string

func (id EventID) IsZero() bool {
	return len(strings.TrimSpace(id.String())) == 0
}

func (id EventID) String() string {
	return string(id)
}
