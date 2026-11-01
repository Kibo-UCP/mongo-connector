# Feature Specification: Bulk Apply Catch-Up Mode for Oplog Replication

**Feature**: Bulk apply catch-up mode (`bulkApplyLagSeconds`)
**Created**: 2026-10-30
**Status**: Implemented
**Input**: "When the mongo connector falls behind, run in bulk_upserts instead of single mode like it does when running from a change date"

## Background

`OplogThread` tails `local.oplog.rs` and applies every relevant entry to each
doc manager serially: `upsert`, `update`, `remove`, `handle_command`, and
`insert_file` are invoked once per oplog entry. Each call costs roughly two
synchronous round-trips to the target system (e.g. a meta write plus the
document write). When the connector resumes from a stale checkpoint or falls
behind during a write burst, it must chew through the entire backlog at this
serial rate — the slowest possible mode exactly when throughput matters most.

`bulk_upsert` already exists on the doc-manager API and is used by the
initial/resume collection dump; in the Kibo Solr doc managers it is
specialized for one-shot reindexing (it exits the process when finished
unless `KIBO_SOLR_REINDEX_CONTINUE_OPLOG_INDEXING` is set). This feature
adds a streaming bulk-apply path that engages automatically when the oplog
thread is far enough behind, **reusing each doc manager's existing
`bulk_upsert`** for upsert batches while suppressing reindex-style
`sys.exit()` calls during catch-up. Doc managers need only small changes
(see FR-013): `bulk_upsert` must chunk its input and raise on failure, it
can check the `_in_bulk_apply` flag to skip one-shot reindex behavior, and
a doc manager can opt into unprojected catch-up fetches with
`catchup_fetch_full_document`. Backlog entries are normalized into per-document
operations and deduplicated to the last op per document within each batch
window, so hot documents cost one write instead of one write per change.

Two related replication gaps in the existing serial path were discovered
while implementing and are addressed as part of this change:

1. **`applyOps` entries were silently dropped.** On MongoDB 5.x+, multi-document
   writes issued as a single command (`insert_many`, `update_many`,
   `delete_many`) are recorded in the oplog as a single `op: "c"` entry with
   `o.applyOps` holding the sub-operations. `handle_command` does not understand
   `applyOps`, so all batched writes were silently lost in both the serial and
   catch-up paths.
2. **`diff`-format update entries were mishandled.** On MongoDB 5.x+, modifier
   updates are recorded as `o: {"$v": N, "diff": {...}}` rather than
   `$set`/`$unset` specs. The catch-up path resolves modifier updates by
   fetching the current document from the source, which sidesteps spec format
   entirely. `filter_oplog_entry` used to treat a `diff` payload as a
   replacement document when `includeFields`/`excludeFields` were configured,
   reducing `o` to `{}`; buffering that as an upsert would have overwritten
   the destination with an `{_id}`-only document. `diff` entries are now left
   intact by field filtering, and any replacement payload with no fields
   besides `_id` is resolved by a source fetch instead of being upserted.
   (The serial `update()` path in `mongo_doc_manager` still mishandles
   `diff` specs — it applies them via `findAndModify` as if they were
   replacement documents — and remains a known pre-existing issue outside this
   feature's scope.)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fast Recovery From a Large Oplog Backlog (Priority: P1)

**Scenario**: As an operations engineer, I need the connector to catch up
quickly after it has fallen behind (restart, maintenance window, write storm),
so that the search index returns to freshness in minutes rather than hours.

**Why this priority**: This is the core value of the feature. Backlog replay is
currently bound by per-entry round-trips; batching collapses thousands of
round-trips into a handful of bulk writes.

**Independent Test**: Seed a replica set with thousands of writes, wait until
they are older than the configured lag threshold, start the connector against a
stale checkpoint, and verify the backlog is applied via `bulk_apply` in batches
and the target system converges to the source.

**Acceptance Scenarios**:

1. **Given** the connector is more than `bulkApplyLagSeconds` behind the newest
   oplog entry, **When** an insert/update/delete entry is processed, **Then**
   the entry is normalized into a buffered op and applied through each doc
   manager's `bulk_apply` method in batches of `bulkApplyBatchSize`
   deduplicated documents
2. **Given** multiple backlog entries touch the same document within a batch
   window, **When** the batch is flushed, **Then** only the last op for that
   document is applied (`i → u → d` applies one remove; `d → i` applies one
   upsert; `i → u → u` applies one upsert)
3. **Given** a batch window of `bulkApplyBatchSize` containing duplicate
   writes, **When** the deduplicated count is below the threshold, **Then**
   the buffer keeps filling until it holds `bulkApplyBatchSize` unique
   documents or a flush boundary is reached
4. **Given** a buffered batch has been applied successfully, **When** the flush
   completes, **Then** the oplog checkpoint advances to the timestamp of the
   newest oplog entry read while the batch was buffered (buffered or
   skipped), and never moves backwards
5. **Given** the connector has caught up to within the lag threshold, **When**
   processing continues, **Then** remaining buffered ops are flushed first and
   subsequent entries resume serial per-entry application

---

### User Story 2 - Correct Replication of All Oplog Entry Types (Priority: P1)

**Scenario**: As a downstream consumer of the replicated data, I need catch-up
mode to produce the same final state as serial mode for every oplog entry
type, so that switching to bulk apply never changes correctness.

**Why this priority**: A speedup that changes semantics is a regression.
Modifier updates, deletes, replacements, commands, GridFS writes, and
`applyOps` batches must all converge to the same target state as before.

**Independent Test**: Generate a mixed backlog (inserts, `$set`/`diff` updates,
replacement updates, deletes, batched writes, collection commands) and verify
the target contains the same final documents as a serial-mode run against the
same oplog.

**Acceptance Scenarios**:

1. **Given** a modifier update entry (`$set`/`$unset` or `diff` format) in the
   backlog, **When** its batch is flushed, **Then** the current version of the
   document is fetched from the source and upserted
2. **Given** a modifier update whose document no longer exists in the source,
   **When** its batch is flushed, **Then** a remove op is applied instead
2a. **Given** a MongoDB 5.x `{"$v": 2, "diff": {...}}` update on a namespace
   with `includeFields`/`excludeFields`, **When** it is buffered, **Then** it
   is resolved by a source fetch (honoring the projection) and never upserted
   as an `{_id}`-only document
3. **Given** a command entry (drop, rename, create, dropDatabase) or a GridFS
   file entry in the backlog, **When** it is reached, **Then** buffered ops are
   flushed first and the command/file op is applied serially, preserving order
4. **Given** an `applyOps` entry containing only CRUD sub-operations, **When**
   it is processed in either mode, **Then** each sub-operation is applied as if
   it were a normal oplog entry at the parent entry's timestamp
5. **Given** an `applyOps` entry containing a non-CRUD sub-operation, **When**
   it is processed, **Then** the parent command entry is handled serially by
   `handle_command` unchanged

---

### User Story 3 - Configurable Threshold With Opt-Out (Priority: P2)

**Scenario**: As a platform administrator, I need the lag threshold to be
configurable per deployment with a way to turn the mode off entirely, so
that a problematic rollout can be reversed with a config change.

**Why this priority**: Safe rollout. The mode is enabled by default at a
conservative 5-minute threshold, so it only engages during meaningful
backlog events; setting it to 0 restores exact pre-feature behavior, and
doc managers without a `bulk_apply` override must still work correctly.

**Independent Test**: Run the connector with `bulkApplyLagSeconds` set to 0
and verify byte-for-byte identical behavior to the previous release; then
verify a doc manager using only the base-class `bulk_apply` (serial
fallback) still receives all ops correctly.

**Acceptance Scenarios**:

1. **Given** `bulkApplyLagSeconds` is explicitly set to 0, **When** any
   oplog entry is processed, **Then** it is applied serially exactly as
   before and `bulk_apply` is never invoked
2. **Given** a doc manager that does not override `bulk_apply`, **When**
   catch-up mode is enabled, **Then** the base-class implementation replays
   ops serially through `upsert`/`remove` and the result is correct
3. **Given** `bulk_upsert` raises `OperationFailed` (or any exception other
   than `ConnectionFailed`) for a batch, **When** the flush runs, **Then**
   that namespace's docs are re-applied one at a time with `upsert`, and a
   single document's `OperationFailed` is logged and skipped as in serial
   mode
4. **Given** a doc manager raises `ConnectionFailed` during a flush (bulk or
   serial fallback), **When** the flush fails, **Then** the buffer is
   discarded, the checkpoint is not advanced, and the oplog is re-read from
   the checkpoint after an exponential backoff (2s doubling, capped at 60s)

---

### User Story 4 - Batched Writes to Solr Targets With Minimal DM Changes (Priority: P2)

**Scenario**: As the owner of the order and shipment search indexes, I need
catch-up mode to reuse the doc managers' existing `bulk_upsert` code, so
that Solr indexing throughput improves during backlog replay with only
small, contract-level changes to the custom doc managers (FR-013).

**Why this priority**: This is where the realized speedup lands for Kibo's
custom doc managers — single-doc `solr.add` calls are the dominant cost
today — and minimizing doc-manager changes reduces rollout risk.

**Independent Test**: Enable catch-up mode with the order doc manager
unmodified, generate a backlog, and verify upserts are batched through the
existing `bulk_upsert` method to the correct core per namespace while
removes keep their existing semantics.

**Acceptance Scenarios**:

1. **Given** the order doc manager, **When** catch-up
   upserts arrive for the order and B2B cores, **Then** cleaned docs are
   batched per Solr core through the DM's existing `bulk_upsert` with the
   same `connector_update` handler and commitWithin settings as a dump
2. **Given** a remove op for the order doc manager, **When** it is
   processed, **Then** it is logged and skipped, mirroring the existing
   `remove` behavior
3. **Given** a doc manager whose `bulk_upsert` exits the process after a
   reindex, **When** catch-up flushes a batch, **Then** the doc manager sees
   `_in_bulk_apply == True` and skips the exit and end-of-job logging; if a
   `SystemExit` is raised anyway, it is suppressed and that namespace's docs
   are applied serially via `upsert` instead of terminating the connector

---

### Edge Cases

- What happens when the lag hovers around `bulkApplyLagSeconds`? (Without
  hysteresis, the connector would apply serially as soon as lag dipped below
  300s, fall behind again, and re-enter bulk mode repeatedly. With
  `bulkApplyExitLagSeconds` (default 60) it stays in bulk mode until lag is
  down to 60s, and only re-enters above `bulkApplyLagSeconds`. Leaving bulk
  mode needs no special handling: the next entry takes the serial path,
  which flushes the buffer first.) Hysteresis narrows the cycle but cannot
  remove it: if serial throughput is below the source write rate, lag grows
  from the exit lag back past `bulkApplyLagSeconds` and bulk mode is entered
  again, only far less often. Persistent cycling means serial application
  cannot keep up; raise `bulkApplyLagSeconds` or lower
  `bulkApplyExitLagSeconds`.
- What happens when the bulk buffer is partially applied and the doc manager
  throws mid-batch? (Checkpoint must not advance past unapplied entries. A
  non-connection error falls back to serial per-doc `upsert`, isolating a
  bad document; reapplication is idempotent. A `ConnectionFailed` discards
  the buffer without checkpointing, and the oplog is re-read from the
  checkpoint after a backoff. If the idle-flush thread hits the failure, it
  sets a flag the run loop checks before reading, applying, or
  checkpointing anything else — including at the top of each tailing pass
  and after each pass over the cursor, so the re-read and backoff happen
  promptly even when the source is quiet and no further entries arrive.)
- What happens when the tailable cursor dies (AutoReconnect/OperationFailure)
  with unflushed buffered ops? (Buffer is discarded; checkpoint is behind, so
  the entries are re-read on reconnect.)
- What happens when a document is updated and deleted within the same batch
  window? (Buffer dedup keeps only the last op per document — `i → u → d`
  collapses to a single remove, `d → i` collapses to a single upsert. For ops
  spanning different flush windows, per-collection ordering in
  `bulk_write(ordered=True)` and per-core op ordering preserve
  last-op-wins.)
- Can dedup lose information? (No — every normalized op carries the
  document's complete final state: upserts carry the full document, modifier
  updates resolve to the current source document at flush time, and removes
  are idempotent. Only the document's *latest* op matters for the target's
  final state.)
- What happens when a fetched document was updated again after its entry but
  before the flush? (The newer state is upserted early; the later entry
  converges identically — replication is convergent, not exact-replay.)
- What happens when a namespace is remapped? (Fetches unmap the destination
  namespace back to the source namespace and apply the configured projection,
  matching dump behavior.)
- What happens on a sharded source? (Fetches go through `mongos_client` when
  present, matching rollback behavior.)
- What happens to skipped entries between buffered ops? (They produce no ops.
  While the buffer is non-empty, a skipped entry's ts is recorded as the
  buffer's trailing timestamp and checkpointed only by a successful flush;
  it never moves the checkpoint past unflushed ops, including when the
  buffer is later discarded after a cursor error. `applyOps` sub-ops share
  their parent's ts, so a skipped sub-op's ts is never checkpointed while a
  sibling sub-op is still buffered.)
- What happens when a flush is due in the middle of an `applyOps` entry?
  (Processing one oplog entry, including every sub-op of an expanded
  `applyOps` entry, is atomic with respect to flushing and checkpointing.
  The run loop holds the catch-up lock for the whole expansion, so the
  idle flush cannot run between siblings, and the `bulkApplyBatchSize`
  flush is deferred until the parent entry is fully processed. A sibling
  that must be applied serially (e.g. a GridFS file write) still flushes the
  siblings buffered before it, to preserve order, but that flush does not
  checkpoint the shared ts; the ts is checkpointed only after every sibling
  has been applied. Otherwise a checkpoint at the parent ts followed by a
  failed flush would lose the remaining siblings, because the checkpointed
  entry is not re-read.)
- What happens to a replacement-style update (`op: "u"` whose `o` is a full
  document) on a namespace with `includeFields`/`excludeFields`?
  (`filter_oplog_entry` trims the payload. Serial mode passes that trimmed
  payload to `update()`, and the Kibo Solr doc managers refetch the full
  source document there. During catch-up the entry is buffered as an
  internal `replace` op. At flush time it becomes a fetch without
  projection for `catchup_fetch_full_document` doc managers, and an upsert
  of the trimmed payload for all other doc managers, with no source query
  when no doc manager needs one. Inserts are unchanged: serial `upsert`
  also receives the filtered document.)
- Can the checkpoint move backwards? (No. During oplog processing the
  checkpoint only moves forward; a stale timestamp recorded before a bulk
  flush (possibly an idle flush on the lag-logger thread) is ignored.
  Only rollback and cursor initialization deliberately move it backwards,
  through `update_checkpoint`.)
- What about compound or embedded-document `_id` values? (The dedup buffer
  and the fetch resolution key documents by the BSON encoding of
  `{"_id": <id>}`, which is hashable; the real ids are used in the `$in`
  query.)
- What happens when the backlog is smaller than `bulkApplyBatchSize` and the
  source goes quiet? (The tailable cursor blocks awaiting new data, so the
  batch would never fill. `bulkApplyIdleSeconds` — default 60 — bounds how
  long buffered ops may sit unflushed; the lag-logger thread checks buffer
  age every 30s and flushes when it exceeds the threshold.)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: When the connector is more than `bulkApplyLagSeconds` behind the
  newest oplog entry, CRUD entries MUST be buffered and applied through each
  doc manager's `bulk_apply` method in batches of at most
  `bulkApplyBatchSize` deduplicated documents
- **FR-001a**: Catch-up mode MUST use hysteresis. It is entered when the
  lag exceeds `bulkApplyLagSeconds`, and once entered it MUST continue until
  the lag is at or below `bulkApplyExitLagSeconds` (default 60). This stops
  the connector flapping between serial and bulk application when it hovers
  around the entry threshold. An exit lag above `bulkApplyLagSeconds` is
  clamped to it (with a warning), which gives single-threshold behavior.
  Entering and leaving the mode are each logged once at INFO.
- **FR-002**: `bulkApplyLagSeconds` MUST default to 300 seconds (5
  minutes); setting it to 0 MUST disable catch-up mode entirely
  (`bulkApplyExitLagSeconds` is then ignored). The
  `bulkApplyLagSeconds`, `bulkApplyExitLagSeconds`, `bulkApplyBatchSize`,
  and `bulkApplyIdleSeconds` options MUST be exposed as
  `--bulk-apply-lag-seconds`, `--bulk-apply-exit-lag-seconds`,
  `--bulk-apply-batch-size`, and `--bulk-apply-idle-seconds` CLI flags and
  JSON config keys. Configuration parsing MUST reject
  `bulkApplyBatchSize < 1` and negative lag, exit-lag or idle seconds with
  `InvalidConfiguration`
- **FR-003**: Buffered ops MUST preserve oplog order end-to-end: a command or
  GridFS entry MUST flush pending buffered ops before being applied serially
- **FR-003a**: The buffer MUST deduplicate ops per document, keeping only the
  last op for each `(namespace, _id)` within a flush window. The key uses
  the BSON encoding of `{"_id": <id>}`, so unhashable (compound/dict) ids
  are supported. Every normalized
  op carries the document's complete state (or resolves to it at flush time),
  so intermediate ops for the same document are redundant. The
  `bulkApplyBatchSize` threshold applies to the count of deduplicated
  documents, so a window containing duplicates keeps filling until it holds
  that many unique documents
- **FR-004**: The checkpoint MUST only advance past entries whose ops were
  successfully flushed or applied. It MUST NOT advance past buffered ops
  that have not been flushed (including via a skipped entry's timestamp),
  MUST NOT advance after a flush that failed with `ConnectionFailed`, and
  MUST NOT move backwards during oplog processing. Processing one oplog
  entry, including every sub-op of an expanded `applyOps` entry, MUST be
  atomic with respect to flushing and checkpointing: no flush (idle,
  batch-size, or before a serially applied sibling) may checkpoint the
  parent ts until every sibling has been applied or buffered
- **FR-005**: Modifier update entries (`$set`/`$unset` and MongoDB 5.x `diff`
  formats) MUST be resolved to the document's current source state via
  batched `$in` fetches honoring the namespace projection; documents missing
  from the source MUST become remove ops. For doc managers that set
  `catchup_fetch_full_document = True` (their serial `update()` refetches
  the whole source document), the fetch MUST omit the projection. Fetches
  are resolved once per kind, so a mix of doc managers costs at most two
  queries per namespace per batch and each doc manager gets the documents
  its serial path would have produced
- **FR-006**: Replacement-style update entries MUST upsert the (field-filtered)
  `o` with `_id` taken from `o2` and `$v` removed, for doc managers that do
  not set `catchup_fetch_full_document`. For doc managers that do, the
  entry MUST instead be resolved by a source fetch without projection (as
  in FR-005), matching their serial `update()`, which refetches the full
  document. No source query is issued for replacement updates when no doc
  manager needs one. A replacement payload with no fields besides `_id`
  (after removing `$v`) MUST be resolved by a source fetch for every doc
  manager, not upserted. `filter_oplog_entry` MUST leave `diff` update
  entries intact. Insert entries keep their filtered payload for every doc
  manager, as serial `upsert` does
- **FR-007**: `applyOps` command entries whose sub-operations are all CRUD ops
  MUST be expanded and processed as normal entries at the parent timestamp,
  in both serial and catch-up modes; any other `applyOps` entry MUST fall
  back to serial command handling
- **FR-008**: `DocManagerBase.bulk_apply(ops)` MUST exist with a default
  implementation that batches consecutive `upsert` ops per namespace
  through the doc manager's existing `bulk_upsert` method and applies
  `remove` ops via `remove` (flushing pending upserts first to preserve
  per-document order). The op tuple format MUST be
  `(op_type, namespace, payload, timestamp)` with `op_type` in
  `{"upsert", "remove"}`; an unknown op type MUST be rejected with
  `ValueError` before any op is applied. The default implementation MUST
  follow this contract, which doc managers rely on:
  - `self._in_bulk_apply` (class attribute, default `False`) MUST be `True`
    for the duration of its calls into `bulk_upsert`/`remove`/`upsert` and
    reset in a `finally`. Reindex-style doc managers use it to skip
    `sys.exit()` and end-of-job logging during catch-up.
  - `bulk_upsert` MUST be called with an iterator of shallow copies,
    `self.bulk_upsert(iter([dict(d) for d in docs]), ns, ts)`, because doc
    managers call `next()` on their input and mutate documents (and refuse
    to clean a document twice). Every serial fallback `upsert` (here and in
    `OplogThread`'s fallbacks) MUST receive a fresh shallow copy, and each
    doc manager gets its own copies.
  - Every `bulk_upsert` and fallback `upsert` in one `bulk_apply` call MUST
    be stamped with the MINIMUM timestamp across the whole `ops` list.
    Kibo's `init_oplog.py` resumes from the target's maximum `_ts`, so a
    newer stamp could make a restart skip older ops in the same call; with
    the minimum, a restart at worst reprocesses ops, which is idempotent.
    Removes keep their own op timestamp. The cost is that a document's
    stored `_ts` can be older than its last op, so a rollback re-sync
    (`rollback()` searches from the target's maximum `_ts`) could miss a
    doc modified shortly after the rollback point.
  - A doc manager MAY opt in to exact per-document timestamps by setting
    `catchup_per_doc_ts = True` (default False). `bulk_apply` then puts
    each document's own last-op timestamp in the reserved key
    `_bulk_apply_ts` (`BULK_APPLY_TS_KEY`) of every document passed to
    `bulk_upsert`, which MUST pop it and stamp with it instead of its
    `timestamp` argument (the argument stays the minimum). Fallback
    `upsert` calls get the document's own timestamp and no reserved key.
    To keep restart from the target's maximum `_ts` safe, each namespace's
    upserts between two removes are applied in ascending timestamp order,
    so an op not yet applied never has a lower timestamp than a `_ts`
    already written (namespaces are separate indexes, so each is ordered
    on its own). Both Kibo doc managers opt in.
  - A `SystemExit` raised by `bulk_upsert` MUST be suppressed (safety net),
    falling back to serial `upsert` for that namespace's docs.
- **FR-009**: Error semantics: if `bulk_upsert` raises `OperationFailed` or
  any other non-`ConnectionFailed` exception, the namespace's docs MUST be
  re-applied serially via `upsert`, and a single document's
  `OperationFailed` MUST be logged and skipped (as is a single remove's).
  If a doc manager's `bulk_apply` itself raises a non-connection exception,
  `OplogThread` MUST replay the batch serially for that doc manager with the
  same per-op handling. `ConnectionFailed`, whether raised in bulk or serial
  application, MUST propagate: the flush discards the buffer, does not
  advance the checkpoint, and the oplog thread re-reads from the checkpoint
  after an exponential backoff (2s doubling, capped at 60s, reset after a
  successful flush)
- **FR-010**: Catch-up upserts MUST be applied via `bulk_upsert` (reusing
  each doc manager's dump code path); any `SystemExit` raised by
  reindex-style `bulk_upsert` implementations MUST NOT terminate the
  connector
- **FR-011**: Unflushed buffered ops MUST be discarded on cursor exceptions;
  they are re-read from the checkpoint after reconnect
- **FR-011a**: Buffered ops MUST be flushed when they have been pending at
  least `bulkApplyIdleSeconds` (default 60; 0 disables), even if the batch
  never fills and no further entries arrive — the check is driven by the
  replication lag thread since the tailable cursor blocks at the tail (and
  the cursor filters out no-op entries, so nothing arrives on an idle
  source). An exception in that thread (e.g. a failed oplog timestamp
  lookup) MUST be logged without stopping the thread.
  Buffer mutation, flush, and serial application MUST be serialized by a
  shared lock so a timer-triggered flush cannot interleave with or reorder
  application of newer entries
- **FR-012**: `mongo_doc_manager` requires no `bulk_apply` override — its
  existing `bulk_upsert` already issues ordered `bulk_write` calls chunked
  at `chunk_size` on data and meta collections
- **FR-013**: The Solr doc managers need small changes to take part
  correctly (made in their own repositories). Their `bulk_upsert` must
  consume the input iterator in chunks and raise (`OperationFailed`, or
  `ConnectionFailed` for connectivity problems) instead of swallowing
  failures, so the connector can isolate bad documents or retry without
  checkpointing. Where `bulk_upsert` calls `sys.exit()` (when
  `KIBO_SOLR_REINDEX_CONTINUE_OPLOG_INDEXING` is unset) or logs end-of-job
  messages, it must skip that while `self._in_bulk_apply` is `True`; the
  connector still suppresses `SystemExit` as a safety net. Doc managers
  whose serial `update()` refetches the full source document should set
  `catchup_fetch_full_document = True` (FR-005). Otherwise catch-up upserts
  are batched identically to a collection dump (same clean-doc,
  commitWithin, handler, and B2B-core-unavailable semantics)
- **FR-014**: Remove ops MUST keep each doc manager's existing delete
  semantics (order DM: log-and-skip; shipment DM: serial `solr.delete`;
  mongo DMs: `remove`), applied after flushing pending upserts

### Key Entities

- **Normalized Op**: `(op_type, namespace, payload, timestamp)` tuple produced
  by `OplogThread` during catch-up; `op_type` is `"upsert"` (payload is a
  full document) or `"remove"` (payload is a document `_id`)
- **Catch-Up Buffer**: Map of `(namespace, _id)` to normalized ops held by
  `OplogThread` between flushes; only the last op per document survives a
  flush window. `fetch` entries are placeholders for modifier updates, and
  `replace` entries carry replacement-update payloads; both are internal
  op types resolved per doc manager kind at flush time (FR-005, FR-006) and
  never reach a doc manager
- **Checkpoint**: BSON timestamp of the last fully-applied oplog entry per
  replica set, stored in the oplog progress dictionary and the oplog file.
  Forward progress goes through `OplogThread._advance_checkpoint`, which
  refuses to move it backwards or to write it while a failed flush is
  pending a re-read; `update_checkpoint` stays unconditional for rollback
  and cursor initialization
- **Meta Document**: Per-document bookkeeping written by the mongo doc
  managers (`_id`/`doc_id`, `ns`, `_ts`) in the `__mongo_connector` database.
  During catch-up, documents upserted by one `bulk_apply` call are stamped
  with the minimum oplog timestamp of that call's ops (FR-008)

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: With catch-up enabled, a backlog of N CRUD entries touching U
  unique documents (U ≤ N) is applied with O(U / bulkApplyBatchSize) bulk
  calls instead of ~2N round-trips, plus at most one `$in` fetch query per
  touched source namespace per batch per projection kind for modifier
  updates (and, for `catchup_fetch_full_document` doc managers,
  replacement updates)
- **SC-002**: Given identical oplog input, catch-up mode produces the same
  final target state as serial mode for all supported entry types
- **SC-003**: With catch-up disabled (`bulkApplyLagSeconds: 0`), behavior is
  identical to the previous release; `bulk_apply` is never invoked. When
  the lag never exceeds the 5-minute default, no ops take the bulk path
- **SC-004**: Batched writes (`insert_many`, `update_many`, `delete_many`)
  replicate correctly on MongoDB 5.x+ where they were previously dropped
- **SC-005**: Checkpoint advances only past flushed entries; a crash or
  cursor failure mid-batch results in re-application, never skipped entries
- **SC-006**: Doc managers lacking a `bulk_apply` override work through the
  default implementation; doc managers whose `bulk_upsert` meets the
  FR-013 contract (chunked iterator input, raises on failure) need no
  further changes

## Out of Scope / Known Issues

- Serial-path handling of `diff`-format update specs in
  `mongo_doc_manager.update()` (applies `{"diff": ...}` as a document
  replacement, corrupting the target doc). Pre-existing; affects both modes
  equally except that catch-up mode resolves updates via source fetches and
  is unaffected.
- `applyOps` entries representing transactions (with `lsid`/`txnNumber`) are
  expanded the same as batched writes; the connector has never had
  transaction semantics.
- `filter_oplog_entry` does not apply include/exclude field filtering to
  `diff` update payloads; it passes them through intact. Catch-up mode
  resolves them with a projected (or, for `catchup_fetch_full_document`
  doc managers, full) source fetch; serial mode passes the unfiltered
  `diff` spec to `update()`, which the Kibo doc managers handle by
  refetching the source document.
- Non-CRUD sub-operations inside `applyOps` fall back to serial
  `handle_command`, which does not apply them.

## Assumptions

- The source MongoDB is a replica set (or sharded cluster) whose oplog is
  tailable; lag is measured in seconds between an entry's `ts` and the newest
  oplog `ts`
- Fetching current documents from the source during catch-up is acceptable
  read load; it is bounded to one `$in` query per touched namespace per batch
- Convergent application (applying a document's current source state) is
  equivalent-or-better than exact replay for this system's targets
- The Kibo doc managers (`mozu_order_solr_doc_manager`,
  `mozu_tenant_filtered_doc_manager`, `kibo_shipment_solr_doc_manager`) are
  deployed with or before this connector version (see Rollout)
- `bulk_apply`, `_in_bulk_apply`, and `catchup_fetch_full_document` are new
  optional doc-manager APIs; older connectors simply never call or set
  them, and doc managers that do not override them inherit the defaults
- A 5-minute default threshold is conservative enough that steady-state
  replication (typically seconds of lag) never engages the bulk path; it
  only activates during genuine backlog events (restarts, outages, write
  storms)

## Rollout

- Catch-up mode is **on by default**: `bulkApplyLagSeconds` defaults to 300
  seconds. Any deployment that picks up this connector version starts
  routing backlog replay through each doc manager's `bulk_upsert` as soon
  as it falls more than 5 minutes behind, with no configuration change. It
  stays in bulk mode until lag is down to `bulkApplyExitLagSeconds`
  (default 60 seconds).
- The Kibo doc manager images MUST ship their FR-013 `bulk_upsert` fixes
  (chunked iterator input, raising `OperationFailed`/`ConnectionFailed`
  instead of swallowing failures, honoring `_in_bulk_apply`, and setting
  `catchup_fetch_full_document` where serial `update()` refetches the full
  source document) **before or with** this connector. Pin each doc manager
  dependency to the tag or commit SHA that contains those fixes, not to a
  branch. Otherwise a swallowed `bulk_upsert` failure could be checkpointed
  past, and reindex-style `bulk_upsert` code paths could run during
  catch-up.
- If the doc manager fixes cannot ship with the connector, set
  `bulkApplyLagSeconds: 0` explicitly in the connector configuration. That
  restores the previous serial behavior exactly (SC-003) until the fixed doc
  managers are deployed.
