package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/thefabric-io/eventsource"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func NewEventStore(tracer trace.Tracer, options *Options) (eventsource.EventStore, error) {
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
		options: options,
		tracer:  tracer,
	}, nil
}

type eventStore struct {
	options *Options
	tracer  trace.Tracer
}

func (s *eventStore) Save(ctx context.Context, t eventsource.Transaction, a eventsource.Aggregate, opts ...eventsource.SaveOption) error {
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.Save")
	defer span.End()

	if t == nil {
		return eventsource.ErrTransactionIsRequired
	}

	tx := t.(*sqlx.Tx)

	options := eventsource.NewSaveOptions(opts...)

	if err := s.save(ctx, tx, a.Changes()); err != nil {
		span.RecordError(err)

		return err
	}

	if options.WithSnapshot {
		snapshots := a.SnapshotsWithFrequency(options.WithSnapshotFrequency)
		if len(snapshots) > 0 {
			if err := s.saveSnapshots(ctx, tx, snapshots...); err != nil {
				span.RecordError(err)

				return err
			}
		}
	}

	return nil
}

func (s *eventStore) Load(ctx context.Context, t eventsource.Transaction, aggregate eventsource.Aggregate) (eventsource.Aggregate, error) {
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.Load")
	defer span.End()

	if aggregate.ID().IsZero() || aggregate.Type().IsZero() {
		return nil, errors.New("aggragate id and type must be specified")
	}

	aggregate.PrepareForLoading()

	if t == nil {
		err := eventsource.ErrTransactionIsRequired

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return nil, err
	}

	tx := t.(*sqlx.Tx)

	latestSnapshot, err := s.loadLatestSnapshot(ctx, tx, aggregate.ID())
	if err != nil && !eventsource.ErrIsSnapshotNotFound(err) {
		span.RecordError(err)

		return nil, err
	}

	snapshotExist := false
	fromVersion := eventsource.AggregateVersion(1)
	if eventsource.ErrIsSnapshotNotFound(err) {
		span.RecordError(err)
	} else {
		fromVersion = latestSnapshot.AggregateVersion.Next()
		snapshotExist = true
	}

	ee, err := s.loadEvents(ctx, tx, aggregate.ID(), fromVersion)
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

	events := aggregate.ParseEvents(ctx, ee...)

	return eventsource.Replay(ctx, aggregate, latestSnapshot, events...)
}

func (s *eventStore) save(ctx context.Context, tx *sqlx.Tx, events []eventsource.Event) error {
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.save")
	defer span.End()

	if len(events) == 0 {
		return eventsource.ErrNoEventsToStore
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
		)

	for _, e := range events {
		sqlEvent, err := FromEvent(e)
		if err != nil {
			return err
		}

		insertBuilder = insertBuilder.Values(
			sqlEvent.ID,
			sqlEvent.Type,
			sqlEvent.OccurredAt,
			sqlEvent.RegisteredAt,
			sqlEvent.AggregateID,
			sqlEvent.AggregateType,
			sqlEvent.AggregateVersion,
			sqlEvent.Data,
			sqlEvent.Metadata,
		)
	}

	query, args, err := insertBuilder.ToSql()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	return nil
}

func (s *eventStore) loadEvents(ctx context.Context, t eventsource.Transaction, id eventsource.AggregateID, fromVersion eventsource.AggregateVersion) ([]eventsource.EventReadModel, error) {
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.loadEvents")
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
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.saveSnapshots")
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
		sqlSnap := FromSnapshot(*snap)
		insertBuilder = insertBuilder.Values(
			sqlSnap.AggregateID,
			sqlSnap.AggregateType,
			sqlSnap.AggregateVersion,
			sqlSnap.TakenAt,
			time.Now().UTC(),
			sqlSnap.Data,
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
	ctx, span := s.tracer.Start(ctx, "eventsource.postgres.eventStore.loadLatestSnapshot")
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
			return nil, eventsource.ErrNoSnapshotFound
		}

		return nil, err
	}

	return snapshot.ToSnapshot(), nil
}

func (s *eventStore) eventsTableName() string {
	return s.computeTableName(s.options.eventStorageParams.tableName)
}

func (s *eventStore) snapshotsTableName() string {
	return s.computeTableName(s.options.snapshotStorageParams.tableName)
}