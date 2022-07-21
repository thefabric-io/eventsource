[![Go Report](https://goreportcard.com/badge/github.com/thefabric-io/eventsource)](https://goreportcard.com/badge/github.com/thefabric-io/eventsource)

# Eventsource

Eventsource is a package helping you to implement eventsourcing concept. 

**This package is not production ready yet.**

A postgresql storage is implemented (also works with coackcroachdb). 

## Concepts:
- Event stream for aggregate
- Outbox pattern
- Snapshot pattern to have a faster event replay
