package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/thefabric-io/errors"
	"github.com/thefabric-io/eventsource"
	"github.com/thefabric-io/timetracker"
)

func NewEventStore(connString, schema string) (eventsource.EventStore, error) {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return nil, err
	}

	return &eventStore{
		conn:   conn,
		schema: schema,
	}, nil
}

type eventStore struct {
	conn              *pgx.Conn
	snapshotFrequency int
	schema            string
}

func (s *eventStore) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

func (s *eventStore) BeginTransaction(ctx context.Context, opts eventsource.BeginTransactionOptions) (eventsource.Transaction, error) {
	return s.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.TxIsoLevel(opts.IsolationLevel),
		AccessMode:     pgx.TxAccessMode(opts.AccessMode),
		DeferrableMode: pgx.TxDeferrableMode(opts.DeferrableMode),
	})
}

func (s *eventStore) Save(ctx context.Context, tx eventsource.Transaction, a eventsource.Aggregator, opts eventsource.SaveOptions) error {
	defer timetracker.Log(time.Now(), "eventstore:save")

	if tx == nil {
		return eventsource.ErrTransactionIsRequired
	}

	events, err := s.save(ctx, tx.(pgx.Tx), s.eventTableName(), a)
	if err != nil {
		return err
	}

	if opts.MustSendToOutbox {
		if err := s.saveToOutbox(ctx, tx, s.outboxTableName(), events); err != nil {
			return err
		}
	}

	if opts.WithSnapshot {
		snapshots := a.SnapshotsWithFrequency(opts.WithSnapshotFrequency)
		if len(snapshots) > 0 {
			if err := s.saveSnapshots(ctx, tx, s.snapshotTableName(), snapshots...); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *eventStore) Load(ctx context.Context, tx eventsource.Transaction, id eventsource.AggregateID, parser eventsource.EventParser) (eventsource.Aggregator, error) {
	defer timetracker.Log(time.Now(), "eventstore:load")

	if tx == nil {
		return nil, eventsource.ErrTransactionIsRequired
	}

	if parser == nil {
		return nil, eventsource.ErrAggregateParserIsRequired
	}

	latestSnapshot, err := s.loadLatestSnapshot(ctx, tx, id)
	if err != nil && !eventsource.ErrIsSnapshotNotFound(err) {
		return nil, err
	}

	snapshotExist := false
	fromVersion := eventsource.AggregateVersion(1)
	if !eventsource.ErrIsSnapshotNotFound(err) {
		fromVersion = latestSnapshot.AggregateVersion.NextVersion()
		snapshotExist = true
	}

	ee, err := s.loadEvents(ctx, tx, id, fromVersion)
	if err != nil {
		return nil, err
	}

	if len(ee) == 0 && !snapshotExist {
		return nil, eventsource.ErrAggregateDoNotExist
	}

	var events = make([]eventsource.Event, 0)
	for _, e := range ee {
		ev := parser.ParseEvent(e)
		events = append(events, ev)
	}

	a := parser.Replay(id, latestSnapshot, events...)

	return a, nil
}

func (s *eventStore) loadEvents(ctx context.Context, t eventsource.Transaction, id eventsource.AggregateID, fromVersion eventsource.AggregateVersion) ([]eventsource.EventReadModel, error) {
	tx := t.(pgx.Tx)

	tableName := s.eventTableName()

	b := strings.Builder{}

	b.WriteString("select id, type, occurred_at, aggregate_id, aggregate_type, aggregate_version, data, metadata, registered_at ")
	b.WriteString(fmt.Sprintf("from %s ", tableName))
	b.WriteString("where aggregate_id = $1 ")
	b.WriteString("and aggregate_version >= $2 ")
	b.WriteString("order by aggregate_version; ")

	query := b.String()

	msg := fmt.Sprintf("loading events for aggregate '%s' from version %d", id, fromVersion)

	log.Println(msg)

	stmt, err := tx.Prepare(ctx, msg, query)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, stmt.Name, id.String(), fromVersion)
	if err != nil {
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
			return nil, err
		}

		events = append(events, event.ToReadModel())
	}

	return events, nil
}

func (s *eventStore) schemaName() string {
	return strings.TrimSpace(s.schema)
}

func (s *eventStore) outboxTableName() string {
	return s.buildTableName("outbox")
}

func (s *eventStore) snapshotTableName() string {
	return s.buildTableName("snapshot")
}

func (s *eventStore) eventTableName() string {
	return s.buildTableName("event")
}

func (s *eventStore) buildTableName(tableName string) string {
	schema := s.schemaName()
	if schema == "" {
		return tableName
	}

	return fmt.Sprintf("%s.%s", schema, tableName)
}

func (s *eventStore) saveSnapshots(ctx context.Context, tx eventsource.Transaction, table string, ss ...*eventsource.Snapshot) error {
	t := tx.(pgx.Tx)

	columns := []string{
		"aggregate_id",
		"aggregate_type",
		"aggregate_version",
		"taken_at",
		"registered_at",
		"data",
	}

	batch := &pgx.Batch{}
	q := fmt.Sprintf("insert into %s (%s) values($1, $2, $3, $4, now(), $5);", table, strings.Join(columns, ", "))
	for _, s := range ss {
		batch.Queue(q,
			s.AggregateID.String(),
			s.AggregateType.String(),
			s.AggregateVersion.Int64(),
			s.TakenAt,
			s.Data,
		)
	}

	result := t.SendBatch(ctx, batch)
	if _, err := result.Exec(); err != nil {
		return err
	}

	defer result.Close()

	return nil
}

func (s *eventStore) loadLatestSnapshot(ctx context.Context, t eventsource.Transaction, id eventsource.AggregateID) (*eventsource.Snapshot, error) {
	tx := t.(pgx.Tx)

	tableName := s.snapshotTableName()

	b := strings.Builder{}

	b.WriteString("select aggregate_id, aggregate_type, aggregate_version, taken_at, data ")
	b.WriteString(fmt.Sprintf("from %s ", tableName))
	b.WriteString("where aggregate_id = $1 ")
	b.WriteString("order by aggregate_version desc ")
	b.WriteString("limit 1; ")

	query := b.String()

	msg := fmt.Sprintf("loading latest snapshot for aggregate '%s'", id)

	log.Println(msg)

	stmt, err := tx.Prepare(ctx, msg, query)
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(ctx, stmt.Name, id.String())

	snapshot := Snapshot{}
	if err := row.Scan(
		&snapshot.AggregateID,
		&snapshot.AggregateType,
		&snapshot.AggregateVersion,
		&snapshot.TakenAt,
		&snapshot.Data,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.Stack(err, eventsource.ErrNoSnapshotFound)
		}
		return nil, err
	}

	return snapshot.ToSnapshot(), nil
}

func (s *eventStore) save(ctx context.Context, tx pgx.Tx, table string, a eventsource.Aggregator) ([]Event, error) {
	columns := []string{
		"id",
		"type",
		"occurred_at",
		"registered_at",
		"aggregate_id",
		"aggregate_type",
		"aggregate_version",
		"data",
		"metadata",
	}

	if len(a.Changes()) == 0 {
		return nil, eventsource.ErrNoEventsToStore
	}

	query, values := buildInsertEvents(table, columns, a.Changes())
	stmt, err := tx.Prepare(ctx, fmt.Sprintf("%v", a.Changes()), query)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, stmt.Name, values...)
	if err != nil {
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
			return nil, err
		}

		eventsDB = append(eventsDB, e)
	}

	return eventsDB, nil
}

func (s *eventStore) saveToOutbox(ctx context.Context, tx eventsource.Transaction, tableName string, ee []Event) error {
	t := tx.(pgx.Tx)

	columns := []string{
		"event_id",
		"registered_at",
		"acknowledged",
		"acknowledged_at",
	}

	batch := &pgx.Batch{}
	q := fmt.Sprintf("insert into %s (%s) values($1, now(), false, null);", tableName, strings.Join(columns, ", "))
	for _, e := range ee {
		batch.Queue(q,
			e.ID,
		)
	}

	result := t.SendBatch(ctx, batch)
	if _, err := result.Exec(); err != nil {
		return err
	}

	defer result.Close()

	return nil
}

func buildInsertEvents(table string, columns []string, events []eventsource.Event) (string, []interface{}) {
	var b strings.Builder
	var aliasNumber int

	eventsCount := len(events)
	if eventsCount == 0 {
		return "", nil
	}

	columnCount := len(columns)

	b.WriteString("insert into " + table + "(" + strings.Join(columns, ", ") + ") values ")

	valueArgs := make([]interface{}, 0, columnCount*eventsCount)
	for eventIndex, e := range events {
		b.WriteString("(")

		aliases := make([]string, columnCount)
		for columnIndex, column := range columns {
			aliasNumber++

			aliases[columnIndex] = fmt.Sprintf("$%d", aliasNumber)
			valueArgs = append(valueArgs, eventValueFromString(column, e))
		}

		b.WriteString(strings.Join(aliases, ", "))
		b.WriteString(")")

		if eventIndex != eventsCount-1 {
			b.WriteString(",")
		}
	}

	b.WriteString(" returning id, type, occurred_at, registered_at, aggregate_id, aggregate_type, aggregate_version, data, metadata;")

	return b.String(), valueArgs
}

func eventValueFromString(s string, event eventsource.Event) interface{} {
	switch s {
	case "id":
		return sql.NullString{
			String: event.ID().String(),
			Valid:  !event.ID().IsZero(),
		}
	case "type":
		return sql.NullString{
			String: event.Type().String(),
			Valid:  !event.Type().IsZero(),
		}
	case "occurred_at":
		return sql.NullTime{
			Time:  event.OccurredAt(),
			Valid: !event.OccurredAt().IsZero(),
		}
	case "registered_at":
		return sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		}
	case "aggregate_id":
		return sql.NullString{
			String: event.AggregateID().String(),
			Valid:  !event.AggregateID().IsZero(),
		}
	case "aggregate_type":
		return sql.NullString{
			String: event.AggregateType().String(),
			Valid:  !event.AggregateType().IsZero(),
		}
	case "aggregate_version":
		return sql.NullInt64{
			Int64: event.AggregateVersion().Int64(),
			Valid: !event.AggregateVersion().IsZero(),
		}
	case "data":
		return event.Data()
	case "metadata":
		return event.Metadata()
	default:
		return nil
	}
}
