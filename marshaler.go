package eventsource

type Marshaler interface {
	MarshalES() ([]byte, error)
	UnmarshalES(b []byte, object any) error
}
