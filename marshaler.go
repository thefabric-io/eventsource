package eventsource

type Marshaler interface {
	MarshalES() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalES(b []byte, object any) error
}
