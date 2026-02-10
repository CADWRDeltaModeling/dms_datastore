# Reconciliation Playground — Narrative Walkthrough

This playground exercises the time-series reconciliation logic in
`dms_datastore.reconcile_data` using small, inspectable repositories.
Each step simulates a realistic operational scenario and shows how
the system responds.

The `archive/` directory captures **before** snapshots and by comparing to the next step you
can see incremental changes directly.

---

## STEP A — Header-only change in staged formatted data

**Scenario**

The upstream formatter re-runs and updates metadata (for example,
`date_formatted`) without changing any numeric values.

**Intent**

Verify that header churn alone does **not** cause repo updates.

**Expected behavior**

- No formatted shards in the repo are rewritten.
- Byte-level differences are ignored when data values are unchanged.

---

## STEP B — Recent value change in staged formatted data

**Scenario**

New or corrected values arrive at the *end* of the time series
(the normal nightly update case).

**Intent**

Exercise incremental updates where staged data should replace
repo data only for the most recent period.

**Expected behavior**

- Only the most recent shard(s) are updated.
- Older history remains untouched.
- The repo reflects the staged values for the recent window.

---

## STEP C — Old history change triggering escalation

**Scenario**

A correction appears deep in historical data
(e.g., reissued archive values from a provider).

**Intent**

Test escalation logic: detecting a change in old history should
force a broader reconcile of recent data to ensure consistency.

**Expected behavior**

- A change in an old shard is detected.
- Reconciliation escalates to include the configured recent window
  (e.g., last 3 years), even if those shards were not directly changed
  in staging.
- This step typically shows a *planned* action before applying.

---

## STEP D — Autoscreened data merged into repo (protecting user overrides)

**Scenario**

Autoscreening is rerun on updated values and produces new screened data
(blank or `user_flag = 1`).

**Intent**

Ensure automated screening integrates cleanly without undoing prior
human decisions.

**Expected behavior**

- Existing explicit user overrides (`user_flag = 0`) in the repo are preserved.
- Autoscreen does **not** erase or override human edits.
- Flags update only where values are new or materially changed.

---

## STEP E — User checkout and return of flagged data

**Scenario**

A user checks out screened data, edits flags manually, and attempts
to push the results back while the repo may have advanced.

**Intent**

Validate safe handling of user edits when data freshness is uncertain.

**Expected behavior**

- User edits (`user_flag = 0` or `1`) are applied **only** where values
  still match the repo.
- If values diverged during checkout, user edits for those timestamps
  are ignored and a warning is issued.
- Repo values always remain authoritative.

## STEP F — Backfill or supplemental data without taking priority

**Scenario**

An auxiliary data source (“special supply”, archive, or real-time feed) is used to
backfill earlier history or fill gaps in the repo.
This source is not considered more authoritative than the existing repo data.

Typical examples:

An archive covering 2007-01-01 to 2007-09-30, supplementing a modern web service that starts on 2007-10-01.

A provisional real-time feed used to fill gaps, but never to override established values.

**Intent**

Demonstrate that lower-priority data can be merged to extend coverage
without overriding existing repo values — and that this backfill persists
across subsequent nightly updates.

**Mechanism**

This is done by running reconciliation with `prefer="repo"` meaning:

  - Repo data remains authoritative where it exists.

  - Staged data fills only gaps or periods outside the repo’s valid range.

  - No temporal “first/last” logic is implied — preference is purely by source.

**Expected behavior** 

In final state:

  - Early history from the supplemental source is prepended into the repo.

  - Existing repo values are never overridden by the supplemental source.

  - Later nightly updates using prefer="staged" do not remove or overwrite the backfilled history.

---

## Notes on interpretation

- “Before” and “after” states for each step can be found under
  `test_repos/archive/<STEP_NAME>/repo`.
- The playground favors **fail-fast, inspectable behavior** over silent
  coercion.
- Numeric formatting in the playground is chosen to make diffs easy
  to read; it does not change reconciliation semantics.

## STEP G — Processed, unsharded update crossing an annual boundary

**Scenario**

A processed product is stored as a **single, unsharded file** that spans an
annual boundary (for example, July–June).

An update arrives that appends new data beyond the existing period of record.
The update may also include revised values within the existing time range.

**Intent**

Exercise reconciliation of unsharded processed data where new timestamps
should be added while existing repo values remain authoritative.

**Mechanism**

Reconciliation is run with:

