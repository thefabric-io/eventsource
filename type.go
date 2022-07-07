package eventsource

import "strings"

type AggregateType string

func (t AggregateType) String() string {
	return string(t)
}

func (t AggregateType) IsZero() bool {
	return len(strings.TrimSpace(string(t))) == 0
}
