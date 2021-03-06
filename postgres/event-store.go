package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/thefabric-io/errors"
	"github.com/thefabric-io/eventsource"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func NewEventStore(db *sqlx.DB, tracer trace.Tracer, options *Options) (eventsource.EventStore, error) {
	if options == nil || options.IsZero() {
		options = DefaultOptions()
	}

	if len(strings.TrimSpace(options.schemaName)) == 0 {
		options.schemaName = "es"
	}

	if err := options.Validate(); err != nil {
		return nil, err
	}

	return &eventStore{
		db:      db,
		options: options,
		tracer:  tracer,
	}, nil
}

type eventStore struct {
	db      *sqlx.DB
	options *Options
	tracer  trace.Tracer
}

func (s *eventStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *eventStore) BeginTransaction(ctx context.Context, opts eventsource.BeginTransactionOptions) (eventsource.Transaction, error) {
	ctx, span := s.tracer.Start(ctx, "eventStore.BeginTransaction")
	defer span.End()

	isolationLevel := s.isolationLevel(opts.IsolationLevel)
	readOnly := s.readOnly(opts.AccessMode)

	span.SetAttributes(attribute.Bool("db.transaction.mode.readOnly", readOnly))
	span.SetAttributes(attribute.String("db.transaction.isolation.level", isolationLevel.String()))

	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  s.readOnly(opts.AccessMode),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	return tx, nil
}

func (s *eventStore) Save(ctx context.Context, t eventsource.Transaction, a eventsource.Aggregator, opts eventsource.SaveOptions) error {
	ctx, span := s.tracer.Start(ctx, "eventStore.Save")
	defer span.End()

	if t == nil {
		return eventsource.ErrTransactionIsRequired
	}

	tx := t.(*sqlx.Tx)

	_, err := s.save(ctx, tx, s.options.eventStorageParams.tableName, a)
	if err != nil {
		span.RecordError(err)

		return err
	}

	if opts.MustSendToOutbox {
		if err := s.saveToOutbox(ctx, tx, a.Changes()); err != nil {
			span.RecordError(err)

			return err
		}
	}

	if opts.WithSnapshot {
		snapshots := a.SnapshotsWithFrequency(opts.WithSnapshotFrequency)
		if len(snapshots) > 0 {
			if err := s.saveSnapshots(ctx, tx, snapshots...); err != nil {
				span.RecordError(err)

				return err
			}
		}
	}

	return nil
}

func (s *eventStore) Load(ctx context.Context, t eventsource.Transaction, id eventsource.AggregateID, parser eventsource.EventParser) (eventsource.Aggregator, error) {
	ctx, span := s.tracer.Start(ctx, "eventStore.Load")
	defer span.End()

	if t == nil {
		err := eventsource.ErrTransactionIsRequired

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	if parser == nil {
		err := eventsource.ErrAggregateParserIsRequired

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	tx := t.(*sqlx.Tx)

	latestSnapshot, err := s.loadLatestSnapshot(ctx, tx, id)
	if err != nil && !eventsource.ErrIsSnapshotNotFound(err) {
		span.RecordError(err)

		return nil, err
	}

	snapshotExist := false
	fromVersion := eventsource.AggregateVersion(1)
	if eventsource.ErrIsSnapshotNotFound(err) {
		span.RecordError(err)
	} else {
		fromVersion = latestSnapshot.AggregateVersion.NextVersion()
		snapshotExist = true
	}

	ee, err := s.loadEvents(ctx, tx, id, fromVersion)
	if err != nil {
		span.RecordError(err)

		return nil, err
	}

	if len(ee) == 0 && !snapshotExist {
		err := eventsource.ErrAggregateDoNotExist

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	var events = make([]eventsource.Event, 0)
	for _, e := range ee {
		ev := parser.ParseEvent(e)
		events = append(events, ev)
	}

	a := parser.Replay(id, latestSnapshot, events...)

	return a, nil
}

func (s *eventStore) save(ctx context.Context, tx *sqlx.Tx, table string, a eventsource.Aggregator) ([]Event, error) {
	ctx, span := s.tracer.Start(ctx, "eventStore.save")
	defer span.End()

	events := a.Changes()
	if len(events) == 0 {
		return nil, eventsource.ErrNoEventsToStore
	}

	insertBuilder := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert(s.eventsTableName()).
		Columns(
			"id",
			"type",
			"occurred_at",
			"registered_at",
			"aggregate_id",
			"aggregate_type",
			"aggregate_version",
			"data",
			"metadata",
		).
		Suffix("returning id, type, occurred_at, registered_at, aggregate_id, aggregate_type, aggregate_version, data, metadata")

	for _, e := range events {
		insertBuilder = insertBuilder.Values(
			sql.NullString{String: e.ID().String(), Valid: !e.ID().IsZero()},
			sql.NullString{String: e.Type().String(), Valid: !e.Type().IsZero()},
			sql.NullTime{Time: e.OccurredAt(), Valid: !e.OccurredAt().IsZero()},
			"now()",
			sql.NullString{String: e.AggregateID().String(), Valid: !e.AggregateID().IsZero()},
			sql.NullString{String: e.AggregateType().String(), Valid: !e.AggregateType().IsZero()},
			sql.NullInt64{Int64: e.AggregateVersion().Int64(), Valid: !e.AggregateVersion().IsZero()},
			e.Data(),
			e.Metadata(),
		)
	}

	query, args, err := insertBuilder.ToSql()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}
	defer rows.Close()

	var eventsDB []Event

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		var e Event

		if err := rows.Scan(
			&e.ID,
			&e.Type,
			&e.OccurredAt,
			&e.RegisteredAt,
			&e.AggregateID,
			&e.AggregateType,
			&e.AggregateVersion,
			&e.Data,
			&e.Metadata,
		); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return nil, err
		}

		eventsDB = append(eventsDB, e)
	}

	return eventsDB, nil
}

func (s *eventStore) loadEvents(ctx context.Context, t eventsource.Transaction, id eventsource.AggregateID, fromVersion eventsource.AggregateVersion) ([]eventsource.EventReadModel, error) {
	ctx, span := s.tracer.Start(ctx, "eventStore.loadEvents")
	defer span.End()

	tx := t.(*sqlx.Tx)

	b := strings.Builder{}

	b.WriteString("select id, type, occurred_at, aggregate_id, aggregate_type, aggregate_version, data, metadata, registered_at ")
	b.WriteString(fmt.Sprintf("from %s ", s.eventsTableName()))
	b.WriteString("where aggregate_id = $1 ")
	b.WriteString("and aggregate_version >= $2 ")
	b.WriteString("order by aggregate_version; ")

	query := b.String()

	rows, err := tx.QueryContext(ctx, query, id.String(), fromVersion)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}
	defer rows.Close()

	events := make([]eventsource.EventReadModel, 0)
	for rows.Next() {
		var event Event
		if err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.OccurredAt,
			&event.AggregateID,
			&event.AggregateType,
			&event.AggregateVersion,
			&event.Data,
			&event.Metadata,
			&event.RegisteredAt,
		); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return nil, err
		}

		events = append(events, event.ToReadModel())
	}

	return events, nil
}

func (s *eventStore) computeTableName(tableName string) string {
	schema := s.options.schemaName
	if schema == "" {
		return tableName
	}

	return fmt.Sprintf("%s.%s", schema, tableName)
}

func (s *eventStore) saveSnapshots(ctx context.Context, tx eventsource.Transaction, ss ...*eventsource.Snapshot) error {
	ctx, span := s.tracer.Start(ctx, "eventStore.saveSnapshots")
	defer span.End()

	t := tx.(*sqlx.Tx)

	insertBuilder := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert(s.snapshotsTableName()).
		Columns(
			"aggregate_id",
			"aggregate_type",
			"aggregate_version",
			"taken_at",
			"registered_at",
			"data",
		)

	for _, snap := range ss {
		insertBuilder = insertBuilder.Values(
			snap.AggregateID.String(),
			snap.AggregateType.String(),
			snap.AggregateVersion.Int64(),
			snap.TakenAt.UTC(),
			time.Now().UTC(),
			string(snap.Data),
		)
	}

	query, args, err := insertBuilder.ToSql()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	_, err = t.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	return nil
}

func (s *eventStore) loadLatestSnapshot(ctx context.Context, tx *sqlx.Tx, id eventsource.AggregateID) (*eventsource.Snapshot, error) {
	ctx, span := s.tracer.Start(ctx, "eventStore.loadLatestSnapshot")
	defer span.End()

	b := strings.Builder{}

	b.WriteString("select aggregate_id, aggregate_type, aggregate_version, taken_at, data ")
	b.WriteString(fmt.Sprintf("from %s ", s.snapshotsTableName()))
	b.WriteString("where aggregate_id = $1 ")
	b.WriteString("order by aggregate_version desc ")
	b.WriteString("limit 1; ")

	query := b.String()

	row := tx.QueryRowContext(ctx, query, id.String())

	snapshot := Snapshot{}
	if err := row.Scan(
		&snapshot.AggregateID,
		&snapshot.AggregateType,
		&snapshot.AggregateVersion,
		&snapshot.TakenAt,
		&snapshot.Data,
	); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		if err == sql.ErrNoRows {
			return nil, errors.Stack(err, eventsource.ErrNoSnapshotFound)
		}

		return nil, err
	}

	return snapshot.ToSnapshot(), nil
}

func (s *eventStore) saveToOutbox(ctx context.Context, tx *sqlx.Tx, ee []eventsource.Event) error {
	ctx, span := s.tracer.Start(ctx, "eventStore.saveToOutbox")
	defer span.End()

	insertBuilder := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert(s.outboxTableName()).
		Columns(
			"event_id",
			"registered_at",
			"acknowledged",
		)

	for _, e := range ee {
		insertBuilder = insertBuilder.Values(e.ID(), "now()", false)
	}

	query, args, err := insertBuilder.ToSql()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	return nil
}

func (s *eventStore) eventsTableName() string {
	return s.computeTableName(s.options.eventStorageParams.tableName)
}

func (s *eventStore) snapshotsTableName() string {
	return s.computeTableName(s.options.snapshotStorageParams.tableName)
}

func (s *eventStore) outboxTableName() string {
	return s.computeTableName(s.options.outboxStorageParams.tableName)
}

func (s *eventStore) isolationLevel(level eventsource.TxIsoLevel) sql.IsolationLevel {
	switch level {
	case eventsource.Serializable:
		return sql.LevelSerializable
	case eventsource.RepeatableRead:
		return sql.LevelRepeatableRead
	case eventsource.ReadCommitted:
		return sql.LevelReadCommitted
	case eventsource.ReadUncommitted:
		return sql.LevelReadUncommitted
	}

	return sql.LevelDefault
}

func (s *eventStore) readOnly(level eventsource.TxAccessMode) bool {
	if level == eventsource.ReadOnly {
		return true
	}

	return false
}
