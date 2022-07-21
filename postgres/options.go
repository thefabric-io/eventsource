package postgres

import (
	"fmt"
	"strings"
)

func DefaultOptions() *Options {
	return &Options{
		schemaName:            defaultSchemaName(),
		eventStorageParams:    defaultEventStorageParams(),
		outboxStorageParams:   defaultOutboxStorageParams(),
		snapshotStorageParams: defaultSnapshotStorageParams(),
	}
}

type Options struct {
	schemaName            string
	eventStorageParams    eventStorageParams
	outboxStorageParams   outboxStorageParams
	snapshotStorageParams snapshotStorageParams
}

func (o *Options) Validate() error {
	if len(strings.TrimSpace(o.schemaName)) == 0 ||
		len(strings.TrimSpace(o.eventStorageParams.tableName)) == 0 ||
		len(strings.TrimSpace(o.outboxStorageParams.tableName)) == 0 ||
		len(strings.TrimSpace(o.snapshotStorageParams.tableName)) == 0 {
		return fmt.Errorf("options invalid")
	}

	return nil
}

func (o *Options) IsZero() bool {
	return *o == Options{}
}

func NewOptionsBuilder() *OptionsBuilder {
	return &OptionsBuilder{options: DefaultOptions()}
}

type OptionsBuilder struct {
	options *Options
}

func (b *OptionsBuilder) WithSchemaName(s string) *OptionsBuilder {
	b.options.schemaName = s

	return b
}

func (b *OptionsBuilder) WithEventStorageTableName(name string) *OptionsBuilder {
	b.options.eventStorageParams.tableName = name

	return b
}

func (b *OptionsBuilder) WithOutboxStorageTableName(name string) *OptionsBuilder {
	b.options.outboxStorageParams.tableName = name

	return b
}

func (b *OptionsBuilder) WithSnapshotStorageTableName(name string) *OptionsBuilder {
	b.options.snapshotStorageParams.tableName = name

	return b
}

func (b *OptionsBuilder) Build() *Options {
	return b.options
}

func defaultSchemaName() string {
	return "es"
}

func defaultEventStorageParams() eventStorageParams {
	return eventStorageParams{tableName: "events"}
}

type eventStorageParams struct {
	tableName string
}

func defaultOutboxStorageParams() outboxStorageParams {
	return outboxStorageParams{
		tableName: "outbox",
	}
}

type outboxStorageParams struct {
	tableName string
}

func defaultSnapshotStorageParams() snapshotStorageParams {
	return snapshotStorageParams{
		tableName: "snapshots",
	}
}

type snapshotStorageParams struct {
	tableName string
}
