# Repository Design: Configuration, Registry, Inventory, and I/O

This document describes the core structure of the repository system used for time series data. It is intended for developers working on adjacent tools (for example inventory, `read_ts_repo`, `dropbox_data`, and update/reconciliation utilities) and emphasizes clarity, determinism, and fail-fast behavior. It expands the earlier design note by adding more explicit treatment of filename interpretation, repository reads, and repository writes. The starting point was your existing outline.  

---

## 1. Repo Configuration

The repo configuration defines how a directory of files is interpreted as structured data.

It is the contract between:

- the filesystem layout
- filename parsing logic (`interpret_fname` and related helpers)
- metadata enrichment from the registry
- downstream read/write tools

A repo config is not just a convenience. It is a formal statement of how filenames map onto logical datasets.

### Example

```yaml
root: //.../repo/continuous/formatted
registry: continuous
key_column: station_id

filename_templates:
  - "{source}_{key@subloc}_{agency_id}_{param}_{year}.csv"

search:
  use_source_slot: true
  shard_style: auto

parse:
  style: legacy

source_priority_mode: by_registry_column
source_priority_column: agency
```

### Main responsibilities of repo configuration

A repo configuration should answer the following questions deterministically:

- Where is the repo rooted?
- Which registry applies?
- What logical key identifies a dataset?
- How are filenames parsed into fields?
- Which fields are optional, merged, or synthetic?
- How do search and prioritization work when more than one file pattern could match?


### `key_column`

`key_column` is the logical identity key used to identify sites and is often the primary registry join key combining station databases with other items like sublocation tables. It identifies the registry entity (kind of like "station") to which a file belongs, but additional fields such as `subloc`, `param`, or `modifier` may still be needed to distinguish logical series within that entity.  This is all very abstract. In current work this key column is often `station_id`, and that column name is ubiquitous in the downloading and processing of continuous time series. However, library/client design should use the registry-named key, not hardcode that name, because for instance `structure_id`, may work better for hydraulic structures.

This key governs:

- inventory aggregation
- registry joins to other tables
- lookup behavior in repo reads
- consistency checks during reconciliation

### `filename_templates`

`filename_templates` map key pieces of metadata to filenames. Since filenames are unique, this is also a defacto form of dataset identity (although it may include extra things). The templates are not only for *writing* names based on metadata; they must also be invertible enough for *reading* names. Often 
once the names are read they are joined to the registry database (e.g. station_dbase.csv) to get other data like georeferencing.
 
That means a template should support two related operations:

1. **Render** a filename from a logical metadata record.
2. **Interpret** a filename back into its fields.

If a template cannot be interpreted reliably, it is not a good repo template. The system is tested for round trips.

---

## 2. Filename Interpretation from Patterns

Filename interpretation is one of the central pieces of repo behavior. It is the bridge between raw files on disk and structured metadata.

### Goal

Given a path such as:

```text
usgs_dsj_11313433_ec_2020.csv
```

and a configured template such as:

```text
{source}_{key@subloc}_{agency_id}_{param}_{year}.csv
```

we want to recover a metadata record such as:

```yaml
source: usgs
key: dsj
subloc: default    # if encoded or inferred by the key@subloc rule
agency_id: 11313433
param: ec
year: 2020
```

### Why this matters

This interpretation step drives:

- inventory building
- repo search
- `read_ts_repo`
- reconciliation/update logic
- sanity checking for malformed filenames

### Pattern slots

A filename template is made up of slots such as:

- `source`
- `key` or `station_id`
- `subloc`
- `agency_id`
- `param`
- `year`

Some slots may be compound, such as `key@subloc`, meaning that the filename contains one piece that must be split or decoded into more than one logical field.

### Important design point

Template interpretation should be **structural**, not heuristic.

That is, the parser should know from configuration:

- which separators are significant
- which slots are present
- which slot is sharding (`year`)
- which slot is logical identity (`key_column`)

It should not guess from arbitrary filenames.

### Output of filename interpretation

Interpreting a filename should produce a structured row or dict that can be used downstream. Typical fields include:

- `filepath`
- parsed fields from the filename
- shard info such as `year`
- family/group identifiers used by inventory

This intermediate parsed result is what feeds the inventory tables and later the registry join.

### Interaction with registry

Filename-derived metadata is intentionally incomplete. Filenames usually contain operational identifiers, not rich metadata. The registry then enriches the parsed filename information with authoritative station metadata.

That is why the flow is:

```text
filename -> parsed fields -> registry join -> enriched metadata
```

not:

```text
filename -> full metadata
```

### Fail-fast principle for filenames

Malformed or non-matching filenames should fail loudly. In particular:

- unexpected numbers of fields
- invalid shard tokens
- impossible combinations of station/param/source
- filenames that do not match any configured template

should be treated as repo hygiene problems, not silently tolerated.

---

## 3. Registry (`station_dbase.csv`)

The registry is the authoritative metadata table for stations.

### Role

It provides:

- station identity
- spatial metadata (lat/lon/x/y)
- naming
- agency ownership
- flags (stage, flow, etc.)
- external IDs (for example `cdec_id`, `wdl_id`)

### Why the registry matters

The registry is what lets the repo remain practical without forcing every file to carry every piece of metadata in its filename.

The filename tells us *which* logical series a file belongs to. The registry tells us what that logical series *means*.

### `station_dbase.csv`

The current practical registry artifact is `station_dbase.csv`. It is worth naming explicitly because adjacent tools will often need to know:

- where station identity comes from
- which join key is expected
- which fields are authoritative versus filename-derived

`station_dbase.csv` is therefore not just a convenience table; it is a foundational source for enrichment and validation.

---

## 4. Inventory System

Inventory scans files and builds structured summaries.

### File Inventory (`repo_file_inventory`)

Describes physical file families.

Typical concerns:

- which files exist
- which years/shards exist
- which filename pattern matched
- what parsed fields were recovered

### Data Inventory (`repo_data_inventory`)

Describes logical datasets independent of source file layout.

Typical concerns:

- what logical datasets exist
- which source families contribute to them
- which parameters and sublocations exist
- where shards begin and end

### Relationship between the two

A file inventory is close to the filesystem. A data inventory is closer to logical datasets.

That separation matters because many downstream actions care about logical datasets, not individual shards.

---

## 5. `read_ts_repo`: Repository Reads

`read_ts_repo` is the canonical way to read a logical time series from the repo.

### Conceptual role

A repo read should hide the details of shard layout and filename matching. A caller should be able to ask for a logical dataset such as:

```python
read_ts_repo("dsj", "ec", repo=repo)
```

and receive the time series assembled from the appropriate file family or families.

### Responsibilities of `read_ts_repo`

At a high level, `read_ts_repo` should:

1. Resolve the user request into a repo search.
2. Use the repo configuration and filename interpretation rules to locate matching files.
3. Respect any source-priority or shard rules.
4. Read the underlying files with the normal file readers.
5. Combine shards into one logical series.
6. Return the final time series in canonical form.

### Why this is important

This function is the boundary between:

- logical dataset lookup
- file-level reading

It should not duplicate reader logic. Instead, it should use the existing readers for the actual file contents and focus on repo-level concerns:

- identifying which files belong
- ordering them
- merging them
- enforcing repo rules

### Interaction with the registry

A repo read often starts from a logical station key, not an agency key. The registry is therefore part of the lookup path whenever translation or enrichment is needed.

For example, station identity and source selection may involve:

- a logical `station_id`
- a registry-derived agency/source relationship
- pattern-based filename search

### Interaction with source priority

Where multiple files could satisfy a request, `read_ts_repo` should follow configured priority rules rather than ad hoc guessing. This is especially important when multiple source families exist for the same logical dataset.

### Design principle

`read_ts_repo` should be boring and deterministic:

- predictable search
- predictable file selection
- predictable merge order
- fail-fast on ambiguity that configuration does not resolve

---

## 6. `write_ts_csv`: Repository Writes and File Emission

`write_ts_csv` is the central CSV writer for files that carry commented YAML metadata headers.

### Conceptual role

This function is responsible for turning:

- a time series table
- optional metadata

into a canonical on-disk CSV representation.

### Why centralization matters

Writers are where formatting drift tends to appear:

- header spacing
- metadata ordering
- date formatting
- nullable flag handling
- newline behavior

A central writer keeps those rules in one place.

### Current expectations for `write_ts_csv`

`write_ts_csv` should handle three metadata modes:

#### 1. `metadata is None`

Synthesize a minimal canonical header, typically including:

- `format`
- `date_formatted`

This is the lightweight path for casual writes.

#### 2. `metadata` is a mapping/dict

Treat the mapping as authoritative metadata.

Important practical behavior:

- if `format` is already present, respect it
- if `format` is absent, insert the default format into a copy
- preserve ordering, with `format` first by convention
- do not mutate the caller’s dict

This is the preferred path for structured writes and future format-version support.

#### 3. `metadata` is a string

Treat it as a prebuilt or semi-structured header text path. This is more legacy/convenience oriented and should be used carefully, since the dict path is the canonical structured path.

### Relationship to `prep_header`

`prep_header` is the serializer for header metadata. `write_ts_csv` is the higher-level file writer that:

- chooses how metadata should be interpreted
- obtains the serialized header text
- writes the header and the data body together

This separation is useful:

- `prep_header` handles header formatting
- `write_ts_csv` handles file emission and convenience/defaulting policy

### Canonicalization goals

A good writer should produce:

- stable header ordering
- canonical comment prefixes (`# `)
- correct handling of `user_flag` and similar columns
- idempotent output under repeated write/read/write cycles

This is what the new round-trip tests are designed to enforce.

---

## 7. End-to-End Flow

```text
FILES (CSV/RDB)
   ↓
interpret_fname
   ↓
metadf (parsed metadata)
   ↓
groupby + aggregate
   ↓
grouped (flattened + normalized)
   ↓
JOIN (registry)
   ↓
metastat (inventory output)
   ↓
read_ts_repo lookup / write_ts_csv emission
```

A slightly more operational view is:

```text
logical request
   ↓
read_ts_repo
   ↓
pattern-based file search
   ↓
interpret_fname
   ↓
read_ts / read_flagged
   ↓
assembled time series
```

and on the write side:

```text
time series + metadata
   ↓
write_ts_csv
   ↓
prep_header
   ↓
canonical commented YAML header + CSV body
```

---

## 8. Design Principles

### No silent fallback configuration

If the repo shape or pattern interpretation is ambiguous, the system should fail rather than silently guess.

### Fail-fast behavior

Bad filenames, malformed headers, and impossible joins should surface immediately.

### Separation of concerns

- filename interpretation is not data reading
- registry enrichment is not filename parsing
- repo lookup is not raw CSV parsing
- canonical writing is not reconciliation policy

### Deterministic behavior

Given the same config, registry, and files, the result should be the same.

### Canonical read/write round-tripping

For repository headers, the long-term goal is:

```text
parse -> serialize -> parse
```

with stable semantics and stable output for canonical files.

---

## 9. Summary

- Configuration defines repo structure.
- Filename templates define how logical datasets map to files.
- `interpret_fname` is the structural bridge from filenames to parsed metadata.
- `station_dbase.csv` enriches parsed file identity with authoritative station metadata.
- Inventory summarizes both physical files and logical datasets.
- `read_ts_repo` is the canonical logical read path.
- `write_ts_csv` is the canonical file-emission path.

All interactions are governed by a strict key (`key_column`), explicit filename interpretation rules, and fail-fast validation.
