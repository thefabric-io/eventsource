package eventsource

type AggregateVersion int

func (v AggregateVersion) Next() AggregateVersion {
	return v + 1
}

func (v AggregateVersion) Int64() int64 {
	return int64(v)
}

func (v AggregateVersion) IsZero() bool {
	return v == 0
}
