# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

## Nodes and agents

### Node

A machine the hub monitors, registered in advance and thereafter identified by the credential it authenticates with. The hub keeps one row per node holding what that machine last reported about itself plus what the hub itself observed about its connection. A node is never discovered on its own; an unregistered credential is not a node.

### Agent

The program installed on a node that connects out to the hub and reports that machine's facts and metrics. The hub never dials a node — the agent holds the connection open, and a node that is not currently connected is simply offline rather than unreachable.

## Addresses

The hub holds two addresses per node that are easy to confuse, because both are exposed to the panel and one is derived from the other. Only one of them is rendered as an address.

### Observed address

Where a node's connection actually arrived from, as the hub saw it at accept time. The node cannot choose it, which is what makes it the only evidence of how a node behind NAT is reachable from outside. Its *shape* depends on which socket the hub listens on: a dual-stack wildcard listener reports an IPv4 peer in a v6 wrapping, so a consumer cannot assume an address's family from the string alone.

### Geo address

The address the hub uses for the node's country lookup, and the key that decides when that lookup goes stale. It prefers an address the agent reported about itself — stable across reconnects, unlike an address observed at connect time — and falls back to the observed address only when the agent reported nothing global.

### Public address

An address globally routable from the internet, as opposed to a private, loopback, link-local, or carrier-grade-NAT one. Which ranges count as public is decided separately in several places in this codebase and those lists are not shared, so the same value can be judged differently by different consumers.
