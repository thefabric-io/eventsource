package eventsource

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func NewID(p IdentityPrefix) AggregateID {
	p = newIDPrefix(string(p))

	return AggregateID(fmt.Sprintf("%s|%s", p[0:idPrefixLen], uuid.NewString()))
}

type AggregateID string

func (id AggregateID) String() string {
	return string(id)
}

func (id AggregateID) IsZero() bool {
	return len(strings.TrimSpace(string(id))) == 0
}

const idPrefixLen = 3

func newIDPrefix(s string) IdentityPrefix {
	if len(s) < idPrefixLen {
		b := strings.Builder{}
		b.WriteString(s)
		for i := 0; i < idPrefixLen; i++ {
			b.WriteString("x")
		}

		s = b.String()
	}

	return IdentityPrefix(s)
}

type IdentityPrefix string

func (p IdentityPrefix) String() string {
	return string(p)
}

func (p IdentityPrefix) IsZero() bool {
	return len(strings.TrimSpace(string(p))) == 0
}

type EventID string

func (id EventID) IsZero() bool {
	return len(strings.TrimSpace(id.String())) == 0
}

func (id EventID) String() string {
	return string(id)
}
