# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys

from mongo_connector.connector import get_mininum_mongodb_version
from mongo_connector.errors import (
    ConnectionFailed,
    OperationFailed,
    UpdateDoesNotApply,
)


LOG = logging.getLogger(__name__)

# Key under which bulk_apply passes a document's own last-op timestamp to
# bulk_upsert, for doc managers that set ``catchup_per_doc_ts``.
BULK_APPLY_TS_KEY = "_bulk_apply_ts"


class DocManagerBase(object):
    """Base class for all DocManager implementations."""

    # True while the default bulk_apply() is running. Doc managers whose
    # bulk_upsert has one-shot reindex behavior (e.g. calling sys.exit() or
    # logging end-of-job messages when finished) should check this flag and
    # skip that behavior during oplog catch-up.
    _in_bulk_apply = False

    # When True, OplogThread resolves modifier/diff updates buffered during
    # bulk catch-up by fetching the WHOLE source document, ignoring the
    # namespace's includeFields/excludeFields projection. Set this on doc
    # managers whose serial update() refetches the full source document, so
    # catch-up produces the same documents as serial application.
    catchup_fetch_full_document = False

    # When True, bulk_apply stamps each document with its own last-op
    # timestamp (passed to bulk_upsert in the BULK_APPLY_TS_KEY key of each
    # document) instead of the minimum timestamp of the whole flush. See
    # bulk_apply() for the contract.
    catchup_per_doc_ts = False

    def apply_update(self, doc, update_spec):
        """Apply an update operation to a document."""

        # Helper to cast a key for a list or dict, or raise ValueError
        def _convert_or_raise(container, key):
            if isinstance(container, dict):
                return key
            elif isinstance(container, list):
                return int(key)
            else:
                raise ValueError

        # Helper to retrieve (and/or create)
        # a dot-separated path within a document.
        def _retrieve_path(container, path, create=False):
            looking_at = container
            for part in path:
                if isinstance(looking_at, dict):
                    if create and part not in looking_at:
                        looking_at[part] = {}
                    looking_at = looking_at[part]
                elif isinstance(looking_at, list):
                    index = int(part)
                    # Do we need to create additional space in the array?
                    if create and len(looking_at) <= index:
                        # Fill buckets with None up to the index we need.
                        looking_at.extend([None] * (index - len(looking_at)))
                        # Bucket we need gets the empty dictionary.
                        looking_at.append({})
                    looking_at = looking_at[index]
                else:
                    raise ValueError
            return looking_at

        def _set_field(doc, to_set, value):
            if "." in to_set:
                path = to_set.split(".")
                where = _retrieve_path(doc, path[:-1], create=True)
                index = _convert_or_raise(where, path[-1])
                wl = len(where)
                if isinstance(where, list) and index >= wl:
                    where.extend([None] * (index + 1 - wl))
                where[index] = value
            else:
                doc[to_set] = value

        def _unset_field(doc, to_unset):
            try:
                if "." in to_unset:
                    path = to_unset.split(".")
                    where = _retrieve_path(doc, path[:-1])
                    index_or_key = _convert_or_raise(where, path[-1])
                    if isinstance(where, list):
                        # Unset an array element sets it to null.
                        where[index_or_key] = None
                    else:
                        # Unset field removes it entirely.
                        del where[index_or_key]
                else:
                    del doc[to_unset]
            except (KeyError, IndexError, ValueError):
                source_version = get_mininum_mongodb_version()
                if source_version is None or source_version.at_least(2, 6):
                    raise
                # Ignore unset errors since MongoDB 2.4 records invalid
                # $unsets in the oplog.
                LOG.warning(
                    "Could not unset field %r from document %r. "
                    "This may be normal when replicating from "
                    "MongoDB 2.4 or the destination could be out of "
                    "sync." % (to_unset, doc)
                )

        # wholesale document replacement
        if "$set" not in update_spec and "$unset" not in update_spec:
            # update spec contains the new document in its entirety
            return update_spec
        else:
            try:
                # $set
                for to_set in update_spec.get("$set", []):
                    value = update_spec["$set"][to_set]
                    _set_field(doc, to_set, value)

                # $unset
                for to_unset in update_spec.get("$unset", []):
                    _unset_field(doc, to_unset)

            except (KeyError, ValueError, AttributeError, IndexError):
                exc_t, exc_v, exc_tb = sys.exc_info()
                msg = "Cannot apply update %r to %r" % (update_spec, doc)
                raise UpdateDoesNotApply(msg).with_traceback(exc_tb)
            return doc

    def bulk_upsert(self, docs, namespace, timestamp):
        """Upsert each document in a set of documents.

        This method may be overridden to upsert many documents at once.
        """
        for doc in docs:
            self.upsert(doc, namespace, timestamp)

    def bulk_apply(self, ops):
        """Apply a sequence of normalized oplog operations.

        ``ops`` is an iterable of ``(op_type, namespace, payload, timestamp)``
        tuples produced by OplogThread while catching up on a large oplog
        backlog (see the ``bulkApplyLagSeconds`` option). ``op_type`` is one
        of:

        - ``"upsert"``: ``payload`` is the full document to upsert. Modifier
          updates (``$set``/``$unset``/``diff`` oplog entries) are resolved
          to whole documents by the OplogThread before reaching this method.
        - ``"remove"``: ``payload`` is the ``_id`` of the document to remove.

        Operations must be applied in the order given. This default
        implementation routes upsert ops through the existing
        :meth:`bulk_upsert` method (batching consecutive upserts per
        namespace, flushed before each remove to preserve ordering) and
        remove ops through :meth:`remove`. Doc managers may override this
        for finer-grained batching (e.g. batching removes).

        Contract for doc managers relying on this default implementation:

        - ``self._in_bulk_apply`` is True for the duration of the call, so
          a doc manager can tell a catch-up ``bulk_upsert`` apart from a
          one-shot reindex ``bulk_upsert`` (e.g. to skip ``sys.exit()``).
        - ``bulk_upsert`` receives an *iterator* of shallow copies of the
          buffered documents; every serial fallback ``upsert`` receives a
          fresh shallow copy too, so a doc manager may mutate what it is
          given.
        - By default every ``bulk_upsert`` and fallback ``upsert`` is
          stamped with the minimum timestamp across the whole ``ops`` list.
          Restarting from the doc manager's newest ``_ts`` then never skips
          an op in this call; reapplying ops is idempotent. The cost is that
          a document's stored timestamp can be older than its last op.
        - A doc manager that sets ``catchup_per_doc_ts = True`` instead gets
          each document's own last-op timestamp, as serial application
          would. Each document passed to ``bulk_upsert`` then carries it in
          the reserved key ``BULK_APPLY_TS_KEY`` (``"_bulk_apply_ts"``),
          which ``bulk_upsert`` must pop from the document and use in place
          of its ``timestamp`` argument. Fallback ``upsert`` calls get the
          document's own timestamp as their argument and no reserved key.
          So that a failure partway through still leaves the doc manager's
          newest ``_ts`` safe to restart from, each namespace's documents
          are applied in ascending timestamp order between removes: every
          document not yet applied has a timestamp at or above the newest
          ``_ts`` already written. ``timestamp`` is still the minimum, for
          doc managers that do not opt in.
        - If ``bulk_upsert`` raises anything other than
          :class:`~mongo_connector.errors.ConnectionFailed`, the
          namespace's documents are re-applied one at a time with
          :meth:`upsert` so a bad document is isolated; a single document's
          :class:`~mongo_connector.errors.OperationFailed` is logged and
          skipped, as in serial oplog application. ``ConnectionFailed`` is
          propagated so the caller does not checkpoint past unapplied ops.
        """
        ops = list(ops)
        for op in ops:
            if op[0] not in ("upsert", "remove"):
                raise ValueError("Unknown bulk_apply op type: %r" % (op[0],))
        if not ops:
            return
        timestamp = min(op[3] for op in ops)
        per_doc_ts = bool(getattr(self, "catchup_per_doc_ts", False))
        pending = {}

        def flush_pending():
            for ns, entries in pending.items():
                if per_doc_ts:
                    # Stable, so ops with equal timestamps keep their order.
                    entries.sort(key=lambda e: e[1])
                self._bulk_apply_upserts(
                    entries, ns, timestamp, per_doc_ts)
            pending.clear()

        self._in_bulk_apply = True
        try:
            for op_type, namespace, payload, op_timestamp in ops:
                if op_type == "upsert":
                    pending.setdefault(namespace, []).append(
                        (payload, op_timestamp))
                else:
                    flush_pending()
                    try:
                        self.remove(payload, namespace, op_timestamp)
                    except OperationFailed:
                        LOG.exception(
                            "Unable to remove document %r from namespace "
                            "'%s' during bulk_apply" % (payload, namespace)
                        )
            flush_pending()
        finally:
            self._in_bulk_apply = False

    def _bulk_apply_upserts(self, entries, namespace, timestamp,
                            per_doc_ts=False):
        """Upsert one namespace's ``(doc, op timestamp)`` entries for
        bulk_apply via bulk_upsert, falling back to per-document upserts on
        failure."""
        def batch():
            for doc, op_ts in entries:
                copy = dict(doc)
                if per_doc_ts:
                    copy[BULK_APPLY_TS_KEY] = op_ts
                yield copy

        try:
            self.bulk_upsert(batch(), namespace, timestamp)
            return
        except SystemExit:
            # Some DocManagers call sys.exit() at the end of bulk_upsert
            # (one-shot reindex semantics). Suppress it during catch-up and
            # apply this namespace's docs serially, since their state after
            # the exit is unknown.
            LOG.warning(
                "bulk_upsert raised SystemExit during bulk_apply "
                "for namespace '%s'; applying docs serially" % namespace
            )
        except ConnectionFailed:
            raise
        except Exception:
            LOG.exception(
                "bulk_upsert failed during bulk_apply for namespace '%s'; "
                "applying %d docs serially" % (namespace, len(entries))
            )
        for doc, op_ts in entries:
            try:
                self.upsert(dict(doc), namespace,
                            op_ts if per_doc_ts else timestamp)
            except OperationFailed:
                LOG.exception(
                    "Unable to upsert document %r into namespace '%s' "
                    "during bulk_apply" % (doc.get("_id"), namespace)
                )

    def update(self, doc, update_spec, namespace, timestamp):
        """Update a document.

        ``update_spec`` is the update operation as provided by an oplog record
        in the "o" field.
        """
        raise NotImplementedError

    def upsert(self, document, namespace, timestamp):
        """(Re-)insert a document."""
        raise NotImplementedError

    def remove(self, document_id, namespace, timestamp):
        """Remove a document.

        ``document_id`` is a dict that provides the id of the document
        to be removed. ``namespace`` and ``timestamp`` provide the database +
        collection name and the timestamp from the corresponding oplog entry.
        """
        raise NotImplementedError

    def insert_file(self, f, namespace, timestamp):
        """Insert a file from GridFS."""
        raise NotImplementedError

    def handle_command(self, command_doc, namespace, timestamp):
        """Handle a MongoDB command."""
        raise NotImplementedError

    def search(self, start_ts, end_ts):
        """Get an iterable of documents that were inserted, updated, or deleted
        between ``start_ts`` and ``end_ts``.
        """
        raise NotImplementedError

    def commit(self):
        """Commit all outstanding writes."""
        raise NotImplementedError

    def get_last_doc(self):
        """Get the document that was modified or deleted most recently."""
        raise NotImplementedError

    def stop(self):
        """Stop all threads started by this DocManager."""
        raise NotImplementedError
