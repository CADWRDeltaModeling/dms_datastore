
---

## `update.md`

```markdown
# Repository Refactor Status

This document summarizes the recent evolution of the repository system and the current refactor status.

It is intended for use when continuing work in a new session.

---

## 1. Main Architectural Shift

The repo system has been moving from a partially hardcoded, partially legacy-driven model to a more explicit and fully config-driven model.

### Main direction

- make repo config authoritative
- eliminate hidden assumptions
- generalize provenance using `provider_key`
- use `site_key` instead of hardcoded site identity names
- keep failure early and explicit

---

## 2. Terminology Shift

### Current terms

- `site_key`
- `provider_key`
- `provider_resolution_mode`

### Terms being phased out

- `key_column`
- hardcoded `source` / `agency` assumptions
- legacy priority naming

Not every caller is fully migrated yet, but the direction is clear.

---

## 3. Configuration Status

`dstore_config.py` has been moving toward a strict repo contract.

The new required config concepts are:

- `site_key`
- `provider_key`
- `provider_resolution_mode`
- `filename_templates`

Tests and mocked configs needed to be updated because older minimal repo fixtures no longer satisfy the new contract.

A common source of failures during this work was stale test fixtures that still used older config shapes.

---

## 4. Filename System Status

`filename.py` has been actively patched.

### Main changes

- naming specs now center on `site_key`
- template parsing/rendering is more literal
- optional `@` suffix handling is treated as structural
- `@subloc` and `@modifier` are optional by design

### Important design decision

A token like:

```text
{station_id@subloc}