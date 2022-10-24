[![Go Report](https://goreportcard.com/badge/github.com/thefabric-io/eventsource)](https://goreportcard.com/badge/github.com/thefabric-io/eventsource)

# Eventsource

## Definition

Event sourcing pattern implemented with a postgresql/coackcroachdb compatible eventstore.

Events, aggregates, snapshots and the full aggregate's current state (last projection based on the last transaction) are persisted using the same transaction.

Commands are not persisted but you still can persist them if needed in another layer.

The package also implement an outbox pattern also persisted in the same transaction.

The **ids** of the entities are generated using a K-Sortable Unique IDentifier (1 second resolution). The migrations to maintain the postgresql eventstore will be added in a future version. The snapshots does not yet have a proper identifier, this should be added at a later stage; snapshots can be fecthed using the aggregate id and the version, subject to a unique index formed by both.

_This version is subject to change and will possibly cause breaking changes._

## Postgresql/CoackcroachDB schema definition

The SQL schema is as follow (a migration script will be included at a later stage):

```postgresql
create schema if not exists es;
create schema if not exists projection;

drop table if exists es.snapshots;
drop table if exists es.events;
drop table if exists projection.organizations;

create table if not exists es.events
(
    id                varchar primary key,
    type              varchar,
    occurred_at       timestamptz,
    registered_at     timestamptz,
    aggregate_id      varchar,
    aggregate_type    varchar,
    aggregate_version bigint,
    data              jsonb,
    metadata          jsonb,
    unique (aggregate_id, aggregate_version)
);

create table if not exists es.snapshots
(
    aggregate_id      varchar,
    aggregate_type    varchar,
    aggregate_version bigint,
    taken_at          timestamptz,
    registered_at     timestamptz,
    data              jsonb,
    primary key (aggregate_id, aggregate_version)
);

create table if not exists projection.organizations
(
    id                  varchar primary key,
    name                varchar,
    registration_number varchar,
    vat_number          varchar,
    vat_is_intra_com    varchar,
    country             varchar,
    created_by          varchar,
    updated_at          timestamptz,
    created_at          timestamptz,
    version             bigint,
    registered_at       timestamptz
)
```