repo.md (current-state, clean)
# Repository Design (Current State)

This document describes the **current design** of the time series repository system.

It is intended for developers and users who need to:
- read data from the repo
- build inventory tools
- implement downloaders or processors
- integrate with registry metadata

This document avoids historical context and focuses on **how the system works today**.

---

## 1. Core Concepts

The repository system is built around four core elements:

1. **Repository Configuration**
2. **Filename Interpretation**
3. **Registry (station_dbase.csv)**
4. **Inventory + I/O (read/write)**

These form a strict, deterministic pipeline:


files → interpret_fname → metadata → registry join → inventory / read_ts_repo


---

## 2. Repository Configuration

The repo configuration defines how a directory of files becomes structured data.

### Example

```yaml
root: //.../repo/continuous/formatted
registry: continuous

provider_key: source
provider_resolution_mode: by_registry_column

filename_templates:
  - "{source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv"

search:
  use_source_slot: true
  shard_style: auto

parse:
  style: legacy
Required Fields

Every repo config must define:

root
registry
provider_key
provider_resolution_mode
filename_templates

Missing any of these is an error.

Key Concepts
station_id

The universal identity column for all sites (stations, structures, synthetic locations).

Used for:

registry joins
inventory grouping
read lookup
provider_key

Data provenance (e.g., source, formerly agency)

Used for:

distinguishing file families
prioritization
provider_resolution_mode

Defines how conflicts are resolved when multiple providers exist.

Typical modes:

by_registry_column
(future: explicit ordering)
3. Filename Templates and Interpretation

Filename templates define the mapping:

metadata ↔ filename

Example:

{source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv
Interpretation

Given:

usgs_dsj_11313433_ec_2020.csv

We recover:

provider: usgs
site: dsj
subloc: default
agency_id: 11313433
param: ec
year: 2020
Design Rules
Parsing is structural, not heuristic
Templates must support:
rendering
interpretation
Fail if a filename does not match any template
4. Registry (station_dbase.csv)

The registry provides authoritative metadata.

Role
enrich filename-derived metadata
provide spatial + descriptive data
define provider relationships
Flow
filename → parsed metadata → registry join → enriched metadata
Important

The registry is authoritative.
Filenames are operational identifiers only.

5. Inventory System

Inventory converts files into structured summaries.

5.1 File Inventory (repo_file_inventory)

Groups by file pattern.

Represents:

physical file families
shard coverage (years)
provider-specific datasets

Key:

file_pattern
5.2 Data Inventory (repo_data_inventory)

Groups by logical dataset identity.

Represents:

unique time series
independent of provider

Key:

series_id
5.3 Series Identity

A series_id is constructed from metadata:

[provider?] | site | subloc | param | modifier?

Provider may be removed depending on context.

5.4 Relationship
Inventory Type	Groups By	Purpose
File Inventory	file_pattern	filesystem view
Data Inventory	series_id	logical dataset view
6. read_ts_repo

Canonical way to read a dataset.

Example
read_ts_repo("dsj", "ec", repo="formatted")
Responsibilities
Resolve request → metadata search
Match filename patterns
Apply provider priority
Read files
Merge shards
Return time series
Design Principles
deterministic
no guessing
no silent fallback
fail on ambiguity
7. write_ts_csv

Canonical writer for repository files.

Metadata Modes
1. None

Creates minimal header:

format
timestamp
2. dict

Preferred mode:

structured metadata
must include or receive format
not mutated
3. string

Legacy/manual header

Guarantees
stable formatting
idempotent round-trip
canonical YAML header
8. End-to-End Flow
Read Path
request
  ↓
read_ts_repo
  ↓
pattern search
  ↓
interpret_fname
  ↓
read_ts
  ↓
merged time series
Inventory Path
files
  ↓
interpret_fname
  ↓
metadata dataframe
  ↓
groupby
  ↓
registry join
  ↓
inventory output
Write Path
time series + metadata
  ↓
write_ts_csv
  ↓
prep_header
  ↓
CSV file
9. Design Principles
Fail Fast
bad filenames → error
bad config → error
No Hidden Behavior
no implicit defaults
no guessing providers
Separation of Concerns
Concern	Component
filename parsing	interpret_fname
metadata enrichment	registry
dataset lookup	read_ts_repo
file writing	write_ts_csv
10. Summary
Repo config defines structure
Filenames define identity
Registry defines meaning
Inventory defines visibility
read_ts_repo defines access
write_ts_csv defines output

Everything is explicit, deterministic, and validated



# 🔄 update.md (evolution + current status)

```markdown
# Repository System – Current Status and Evolution

This document captures **where we are now**, what changed recently, and what remains in motion.

It is intended as a bridge between sessions or for onboarding developers into active work.

---

## 1. Major Design Shift

### Old Model
- implicit "agency" logic
- legacy read_multi priority system
- hardcoded behavior for repo tiers
- partial config-driven system

---

### New Model
- fully **config-driven repo interpretation**
- generalized **provider model** (replaces agency/source confusion)
- strict **fail-fast validation**
- explicit **inventory layer**
- deterministic **read_ts_repo**

---

## 2. Key Renames and Concepts

| Old Term | New Term   | Notes |
|----------|------------|------|
| agency   | provider   | generalizes provenance |
| key_column | station_id (hardcoded) | universal identity column |
| source_priority | provider_resolution_mode | config-driven |

---

## 3. Configuration Maturity

### Now Required

Configs must define:

- `provider_key`
- `provider_resolution_mode`
- `filename_templates`

The site identity column is always `station_id` and is no longer configurable.

---

### Implication

There is no longer:
- implicit provider logic
- fallback behavior

Misconfigured repos fail immediately.

---

## 4. Inventory Refactor

### Previously
- loosely defined
- partially coupled to legacy parsing

### Now

Two explicit layers:

#### File Inventory
- grouped by `file_pattern`
- preserves provider

#### Data Inventory
- grouped by `series_id`
- optionally removes provider

---

### Key Improvement

Inventory now acts as a **bridge between repo and tooling**, enabling:

- filtering
- selection
- reproducible dataset identification

---

## 5. Filename Parsing

### Improved

- fully template-driven
- no guessing
- strict failure on mismatch

---

### Remaining Tension

- legacy-style parsing still present (`parse.style = legacy`)
- eventual move toward fully declarative parsing likely

---

## 6. read_ts_repo Evolution

### Old
- priority lists
- agency lookup
- pattern construction

### New Direction
- pattern discovery via config
- wildcard + inventory-driven selection
- provider resolution via config

---

### Status

- partially implemented
- still interacting with legacy assumptions in places

---

## 7. write_ts_csv and Headers

### Stabilized

- YAML header round-trip now consistent
- strict parsing available
- metadata dict is canonical path

---

### Remaining Work

- eliminate legacy string-header paths
- enforce stricter schema if desired

---

## 8. Known Fragile / Active Areas

### 1. Inventory Functions
Recent breakages showed:
- missing functions (`repo_file_inventory`)
- missing helpers (`_drop_inventory_noise`)
- signature drift (`to_wildcard`)

→ These are mechanical issues, not conceptual problems

---

### 2. Backward Compatibility Layer

Functions now accept:
- `remove_source`
- `remove_provider`

This is transitional.

---

### 3. Processed Repo

Still evolving:

- may use:

(site, subloc, param, modifier)

- provider semantics less clear
- config needs to fully define identity

---

### 4. Provider Resolution

Currently:
- registry-driven or simple modes

Future:
- more explicit ordering
- possibly weighted or conditional logic

---

## 9. Design Direction (Clear)

### What We Want

- config defines everything

## 10. Configuration System (Summary)

Repository configs live as YAML under `dms_datastore/config_data/` and are the single source of truth for:

- which directory is the repo root
- how filenames map to metadata (`filename_templates`)
- the registry to use for enrichment
- shard and search behavior (`search` / `parse`)

Configs are validated and consumed by inventory builders, `read_ts_repo`, `populate_repo`, and inbound systems such as `dropbox_data.py`.

## 11. Downloaders and Staging

Downloaders (e.g., `download_nwis.py`) fetch raw agency data and write it to a `raw/` staging area. They intentionally do not perform final renaming or provider-resolution — that is the job of `populate_repo` / `reformat` which consult the repo config to produce canonical files under `formatted/`.

Staged directories used by the project:

- `raw/`: ingest and archived downloads
- `formatted/`: canonical, template-named CSVs created by `populate_repo` / `reformat`
- `screened/`: outputs from `auto_screen.py` and manual review

## 12. Dropbox and `populate_repo`

Dropbox ingestion downloads inbound files into the configured `raw` area and annotates them with source metadata. `populate_repo` reads those raw artifacts, uses the repo config to interpret or map incoming metadata to template slots, and writes canonical CSVs into `formatted/`. Inventory is then updated so the new series are discoverable.

## 13. Operational Notes

- Keep downloaders focused on data retrieval and minimal normalization; avoid embedding repo-specific filename logic.
- Use `populate_repo` to convert raw artifacts into repo-shaped files so that `read_ts_repo` and downstream tools behave deterministically.
- When modifying templates or provider-resolution modes, run inventory and round-trip tests to catch parsing regressions.