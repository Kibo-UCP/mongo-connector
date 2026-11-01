# Copyright 2026 Kibo Commerce
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

"""Unit tests for bulk catch-up mode that do not need a MongoDB cluster.

The source cluster, oplog cursor and doc managers are all mocked.
"""

import sys
import threading
from unittest import mock

import bson
import pymongo

sys.path[0:0] = [""]  # noqa

from mongo_connector import errors
from mongo_connector.doc_managers.doc_manager_base import (
    BULK_APPLY_TS_KEY,
    DocManagerBase,
)
from mongo_connector.locking_dict import LockingDict
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.oplog_manager import (
    CatchupFlushFailed,
    OplogThread,
    ReplicationLagLogger,
)
from mongo_connector.util import bson_ts_to_long
from tests import unittest


class RecordingDocManager(DocManagerBase):
    """DocManager recording every call, and whether _in_bulk_apply was set.

    Like the Kibo Solr doc managers, bulk_upsert consumes its input with
    next() and upsert/bulk_upsert mutate the documents they are given,
    refusing documents that were already mutated.
    """

    def __init__(self):
        self.calls = []
        self.bulk_upsert_error = None
        self.upsert_errors = {}
        self.remove_error = None

    def _clean(self, doc):
        if "ns" in doc or "_ts" in doc:
            raise AssertionError("document was already cleaned: %r" % doc)

    def bulk_upsert(self, docs, namespace, timestamp):
        self.calls.append(
            ("bulk_upsert_input", type(docs), namespace, timestamp,
             self._in_bulk_apply)
        )
        received = []
        while True:
            try:
                doc = next(docs)
            except StopIteration:
                break
            self._clean(doc)
            received.append(dict(doc))
            doc["ns"] = namespace
            doc["_ts"] = timestamp
        if self.bulk_upsert_error is not None:
            raise self.bulk_upsert_error
        self.calls.append(
            ("bulk_upsert", received, namespace, timestamp, self._in_bulk_apply)
        )

    def upsert(self, doc, namespace, timestamp):
        self._clean(doc)
        error = self.upsert_errors.get(doc["_id"])
        if error is not None:
            raise error
        self.calls.append(
            ("upsert", dict(doc), namespace, timestamp, self._in_bulk_apply)
        )
        doc["ns"] = namespace
        doc["_ts"] = timestamp

    def remove(self, document_id, namespace, timestamp):
        if self.remove_error is not None:
            raise self.remove_error
        self.calls.append(
            ("remove", document_id, namespace, timestamp, self._in_bulk_apply)
        )

    def calls_named(self, name):
        return [c for c in self.calls if c[0] == name]


class TestDocManagerBaseBulkApply(unittest.TestCase):
    def setUp(self):
        self.dm = RecordingDocManager()

    def test_bulk_upsert_gets_iterator_of_shallow_copies(self):
        doc = {"_id": 1, "a": 1}
        self.dm.bulk_apply([("upsert", "db.coll", doc, 5)])
        input_call = self.dm.calls_named("bulk_upsert_input")[0]
        self.assertNotIn(input_call[1], (list, tuple))
        self.assertTrue(hasattr(input_call[1], "__next__"))
        self.assertEqual(
            self.dm.calls_named("bulk_upsert")[0][1], [{"_id": 1, "a": 1}]
        )
        # The doc manager mutated a copy, not the buffered document.
        self.assertEqual(doc, {"_id": 1, "a": 1})

    def test_each_doc_manager_gets_its_own_copies(self):
        doc = {"_id": 1, "a": 1}
        ops = [("upsert", "db.coll", doc, 5)]
        dm2 = RecordingDocManager()
        self.dm.bulk_apply(ops)
        # Would raise in _clean if the first doc manager's mutation leaked.
        dm2.bulk_apply(ops)
        self.assertEqual(len(dm2.calls_named("bulk_upsert")), 1)
        self.assertEqual(doc, {"_id": 1, "a": 1})

    def test_in_bulk_apply_flag(self):
        self.assertFalse(DocManagerBase._in_bulk_apply)
        self.assertFalse(self.dm._in_bulk_apply)
        self.dm.bulk_apply(
            [
                ("upsert", "db.coll", {"_id": 1}, 5),
                ("remove", "db.coll", 2, 6),
            ]
        )
        self.assertTrue(all(c[-1] for c in self.dm.calls))
        self.assertFalse(self.dm._in_bulk_apply)

    def test_in_bulk_apply_flag_reset_on_error(self):
        self.dm.bulk_upsert_error = errors.ConnectionFailed("down")
        with self.assertRaises(errors.ConnectionFailed):
            self.dm.bulk_apply([("upsert", "db.coll", {"_id": 1}, 5)])
        self.assertFalse(self.dm._in_bulk_apply)

    def test_stamps_upserts_with_minimum_timestamp(self):
        self.dm.bulk_apply(
            [
                ("upsert", "db.a", {"_id": 1}, 30),
                ("upsert", "db.b", {"_id": 2}, 10),
                ("remove", "db.a", 3, 40),
                ("upsert", "db.a", {"_id": 4}, 20),
            ]
        )
        bulk_calls = self.dm.calls_named("bulk_upsert")
        self.assertEqual([c[3] for c in bulk_calls], [10, 10, 10])
        # Order is preserved around the remove.
        self.assertEqual(
            [c[0] for c in self.dm.calls if c[0] != "bulk_upsert_input"],
            ["bulk_upsert", "bulk_upsert", "remove", "bulk_upsert"],
        )
        # Removes keep their own timestamp.
        self.assertEqual(self.dm.calls_named("remove")[0][3], 40)

    def test_operation_failed_falls_back_to_serial_upserts(self):
        self.dm.bulk_upsert_error = errors.OperationFailed("bad batch")
        self.dm.upsert_errors[2] = errors.OperationFailed("bad doc")
        docs = [{"_id": i, "v": i} for i in (1, 2, 3)]
        self.dm.bulk_apply([("upsert", "db.coll", d, 7 + d["_id"]) for d in docs])
        upserts = self.dm.calls_named("upsert")
        # Doc 2 is isolated and skipped; the rest are applied with the
        # minimum timestamp, from fresh copies.
        self.assertEqual([c[1]["_id"] for c in upserts], [1, 3])
        self.assertEqual({c[3] for c in upserts}, {8})
        self.assertEqual(docs[0], {"_id": 1, "v": 1})

    def test_other_exception_falls_back_to_serial_upserts(self):
        self.dm.bulk_upsert_error = ValueError("unexpected")
        self.dm.bulk_apply([("upsert", "db.coll", {"_id": 1}, 5)])
        self.assertEqual(len(self.dm.calls_named("upsert")), 1)

    def test_system_exit_falls_back_to_serial_upserts(self):
        self.dm.bulk_upsert_error = SystemExit(0)
        self.dm.bulk_apply([("upsert", "db.coll", {"_id": 1}, 5)])
        self.assertEqual(len(self.dm.calls_named("upsert")), 1)

    def test_connection_failed_propagates_without_fallback(self):
        self.dm.bulk_upsert_error = errors.ConnectionFailed("down")
        with self.assertRaises(errors.ConnectionFailed):
            self.dm.bulk_apply([("upsert", "db.coll", {"_id": 1}, 5)])
        self.assertEqual(self.dm.calls_named("upsert"), [])

    def test_serial_connection_failed_propagates(self):
        self.dm.bulk_upsert_error = errors.OperationFailed("bad batch")
        self.dm.upsert_errors[1] = errors.ConnectionFailed("down")
        with self.assertRaises(errors.ConnectionFailed):
            self.dm.bulk_apply([("upsert", "db.coll", {"_id": 1}, 5)])

    def test_remove_operation_failed_is_skipped(self):
        self.dm.remove_error = errors.OperationFailed("missing")
        self.dm.bulk_apply(
            [("remove", "db.coll", 1, 5), ("upsert", "db.coll", {"_id": 2}, 6)]
        )
        self.assertEqual(len(self.dm.calls_named("bulk_upsert")), 1)

    def test_unknown_op_type_rejected_before_applying(self):
        with self.assertRaises(ValueError):
            self.dm.bulk_apply(
                [("upsert", "db.coll", {"_id": 1}, 5), ("bogus", "db.coll", {}, 6)]
            )
        self.assertEqual(self.dm.calls, [])

    def test_catchup_fetch_full_document_default(self):
        self.assertFalse(DocManagerBase.catchup_fetch_full_document)


class PerDocTsDocManager(RecordingDocManager):
    """Opted-in doc manager: pops each doc's own timestamp, as the Kibo
    Solr doc managers do, and stamps it instead of the batch timestamp."""

    catchup_per_doc_ts = True

    def bulk_upsert(self, docs, namespace, timestamp):
        stamped = []
        for doc in docs:
            stamped.append((doc["_id"], doc.pop(BULK_APPLY_TS_KEY)))
        if self.bulk_upsert_error is not None:
            raise self.bulk_upsert_error
        self.calls.append(("bulk_upsert", stamped, namespace, timestamp))


class TestPerDocTimestamps(unittest.TestCase):
    def setUp(self):
        self.dm = PerDocTsDocManager()

    def test_each_doc_stamped_with_its_own_ts_in_ascending_order(self):
        self.dm.bulk_apply([
            ("upsert", "db.coll", {"_id": "a"}, 9),
            ("upsert", "db.coll", {"_id": "b"}, 3),
            ("upsert", "db.coll", {"_id": "c"}, 6),
        ])
        (call,) = self.dm.calls_named("bulk_upsert")
        self.assertEqual(call[1], [("b", 3), ("c", 6), ("a", 9)])
        # The timestamp argument stays the minimum for the whole call.
        self.assertEqual(call[3], 3)

    def test_sorted_per_segment_between_removes(self):
        self.dm.bulk_apply([
            ("upsert", "db.coll", {"_id": "a"}, 9),
            ("upsert", "db.coll", {"_id": "b"}, 3),
            ("remove", "db.coll", "x", 5),
            ("upsert", "db.coll", {"_id": "d"}, 7),
            ("upsert", "db.coll", {"_id": "e"}, 6),
        ])
        names = [c[0] for c in self.dm.calls]
        self.assertEqual(names, ["bulk_upsert", "remove", "bulk_upsert"])
        first, _, second = self.dm.calls
        self.assertEqual(first[1], [("b", 3), ("a", 9)])
        self.assertEqual(second[1], [("e", 6), ("d", 7)])

    def test_namespaces_are_ordered_independently(self):
        self.dm.bulk_apply([
            ("upsert", "db.one", {"_id": "a"}, 8),
            ("upsert", "db.two", {"_id": "b"}, 2),
            ("upsert", "db.one", {"_id": "c"}, 4),
        ])
        by_ns = {c[2]: c[1] for c in self.dm.calls_named("bulk_upsert")}
        self.assertEqual(by_ns["db.one"], [("c", 4), ("a", 8)])
        self.assertEqual(by_ns["db.two"], [("b", 2)])

    def test_serial_fallback_uses_own_ts_and_no_reserved_key(self):
        self.dm.bulk_upsert_error = ValueError("boom")
        self.dm.bulk_apply([
            ("upsert", "db.coll", {"_id": "a"}, 9),
            ("upsert", "db.coll", {"_id": "b"}, 3),
        ])
        upserts = self.dm.calls_named("upsert")
        self.assertEqual([(c[1]["_id"], c[3]) for c in upserts],
                         [("b", 3), ("a", 9)])
        for c in upserts:
            self.assertNotIn(BULK_APPLY_TS_KEY, c[1])

    def test_not_opted_in_keeps_minimum_and_no_reserved_key(self):
        dm = RecordingDocManager()
        dm.bulk_apply([
            ("upsert", "db.coll", {"_id": "a"}, 9),
            ("upsert", "db.coll", {"_id": "b"}, 3),
        ])
        (call,) = dm.calls_named("bulk_upsert")
        self.assertEqual(call[3], 3)
        self.assertEqual([d["_id"] for d in call[1]], ["a", "b"])
        for d in call[1]:
            self.assertNotIn(BULK_APPLY_TS_KEY, d)

    def test_source_docs_not_mutated(self):
        doc = {"_id": "a"}
        self.dm.bulk_apply([("upsert", "db.coll", doc, 4)])
        self.assertEqual(doc, {"_id": "a"})


class FakeCursor(object):
    """Oplog cursor yielding ``entries`` then optionally raising ``error``."""

    def __init__(self, entries, error=None):
        self.entries = entries
        self.error = error
        self.alive = True

    def __iter__(self):
        yield from self.entries
        self.alive = False
        if self.error is not None:
            raise self.error


def ts(n):
    return bson.Timestamp(n, 0)


def insert(n, ns="test.test", doc_id=None):
    return {
        "ts": ts(n),
        "op": "i",
        "ns": ns,
        "o": {"_id": n if doc_id is None else doc_id, "n": n},
    }


def skipped(n):
    # Not in the namespace configuration, so _should_skip_entry skips it.
    return {"ts": ts(n), "op": "i", "ns": "other.coll", "o": {"_id": n}}


class OplogThreadTestBase(unittest.TestCase):
    def make_opman(self, doc_managers, namespace_config=None, **kwargs):
        client = mock.MagicMock()
        client.admin.command.return_value = {"setName": "rs0"}
        if namespace_config is None:
            namespace_config = NamespaceConfig(namespace_options={"test.*": True})
        opman = OplogThread(
            primary_client=client,
            doc_managers=doc_managers,
            oplog_progress_dict=LockingDict(),
            namespace_config=namespace_config,
            **kwargs
        )
        return opman

    def source_collection(self, opman):
        """The mocked collection returned for any source namespace."""
        db = opman.primary_client.__getitem__.return_value
        return db.with_options.return_value.__getitem__.return_value

    def run_with_cursors(self, opman, cursors, is_behind=None):
        """Run the OplogThread over ``cursors`` (one per (re)connect) and
        return the list of checkpoints written, in order.

        ``is_behind(entry)`` decides whether an entry takes the catch-up
        path; by default every entry does."""
        cursors = list(cursors)
        written = []
        original_update = opman.update_checkpoint

        def record_update(checkpoint):
            written.append(checkpoint)
            original_update(checkpoint)

        def init_cursor():
            if not cursors:
                opman.running = False
                return None, True
            return cursors.pop(0), False

        opman.update_checkpoint = record_update
        opman.init_cursor = init_cursor
        if is_behind is None:
            is_behind = lambda entry: True  # noqa: E731
        opman._is_behind_oplog = is_behind
        with mock.patch("mongo_connector.oplog_manager.time.sleep"):
            opman.run()
        return written


class TestBufferAndFilter(OplogThreadTestBase):
    def setUp(self):
        self.opman = self.make_opman([RecordingDocManager()])

    def buffered(self):
        return list(self.opman._catchup_ops.values())

    def test_filter_leaves_diff_update_intact(self):
        entry = {
            "op": "u",
            "ns": "test.test",
            "o2": {"_id": 1},
            "o": {"$v": 2, "diff": {"u": {"a": 1, "z": 2}}},
        }
        result = self.opman.filter_oplog_entry(entry, include_fields=["a"])
        self.assertEqual(result["o"], {"$v": 2, "diff": {"u": {"a": 1, "z": 2}}})
        result = self.opman.filter_oplog_entry(entry, exclude_fields=["z"])
        self.assertEqual(result["o"], {"$v": 2, "diff": {"u": {"a": 1, "z": 2}}})

    def test_diff_update_with_include_fields_buffers_fetch(self):
        opman = self.make_opman(
            [RecordingDocManager()],
            NamespaceConfig(
                namespace_options={"test.test": {"includeFields": ["a"]}}
            ),
        )
        entry = {
            "ts": ts(1),
            "op": "u",
            "ns": "test.test",
            "o2": {"_id": 1},
            "o": {"$v": 2, "diff": {"u": {"z": 2}}},
        }
        skip, _ = opman._should_skip_entry(entry)
        self.assertFalse(skip)
        opman._buffer_oplog_entry(entry, entry["ns"], 1)
        self.assertEqual(
            list(opman._catchup_ops.values()), [("fetch", "test.test", 1, 1)]
        )

    def test_id_only_replacement_buffers_fetch(self):
        for payload in ({}, {"_id": 1}, {"$v": 1}, {"$v": 1, "_id": 1}):
            self.opman._catchup_ops = {}
            entry = {
                "ts": ts(1),
                "op": "u",
                "ns": "test.test",
                "o2": {"_id": 1},
                "o": payload,
            }
            self.opman._buffer_oplog_entry(entry, "test.test", 1)
            self.assertEqual(self.buffered(), [("fetch", "test.test", 1, 1)])

    def test_replacement_buffers_replace(self):
        entry = {
            "ts": ts(1),
            "op": "u",
            "ns": "test.test",
            "o2": {"_id": 1},
            "o": {"$v": 1, "a": 2},
        }
        self.opman._buffer_oplog_entry(entry, "test.test", 1)
        self.assertEqual(
            self.buffered(), [("replace", "test.test", {"a": 2, "_id": 1}, 1)]
        )

    def test_compound_ids_are_buffered_and_deduplicated(self):
        id_a = {"order": 1, "line": 2}
        id_b = {"line": 2, "order": 1}  # a different _id in MongoDB
        self.opman._buffer_oplog_entry(insert(1, doc_id=id_a), "test.test", 1)
        self.opman._buffer_oplog_entry(insert(2, doc_id=id_b), "test.test", 2)
        self.opman._buffer_oplog_entry(
            {"ts": ts(3), "op": "d", "ns": "test.test", "o": {"_id": id_a}},
            "test.test",
            3,
        )
        self.opman._buffer_oplog_entry(insert(4, doc_id=[1, 2]), "test.test", 4)
        self.assertEqual(
            self.buffered(),
            [
                ("remove", "test.test", id_a, 3),
                ("upsert", "test.test", {"_id": id_b, "n": 2}, 2),
                ("upsert", "test.test", {"_id": [1, 2], "n": 4}, 4),
            ],
        )

    def test_compound_id_fetch_resolution(self):
        present = {"order": 1, "line": 1}
        missing = {"order": 1, "line": 2}
        collection = self.source_collection(self.opman)
        collection.find.return_value = [{"_id": dict(present), "a": 1}]
        ops = [
            ("fetch", "test.test", present, 1),
            ("fetch", "test.test", missing, 2),
        ]
        resolved = self.opman._resolve_catchup_fetches(ops)
        self.assertEqual(
            resolved,
            [
                ("upsert", "test.test", {"_id": present, "a": 1}, 1),
                ("remove", "test.test", missing, 2),
            ],
        )
        query = collection.find.call_args[0][0]
        self.assertEqual(
            sorted(query["_id"]["$in"], key=lambda d: d["line"]),
            [present, missing],
        )


class TestCatchupFetchFullDocument(OplogThreadTestBase):
    def test_projection_per_doc_manager_kind(self):
        projected_dm = RecordingDocManager()
        full_dm = RecordingDocManager()
        full_dm.catchup_fetch_full_document = True
        opman = self.make_opman(
            [projected_dm, full_dm],
            NamespaceConfig(
                namespace_options={"test.test": {"includeFields": ["a"]}}
            ),
        )
        collection = self.source_collection(opman)

        def find(query, projection=None):
            if projection is None:
                return [{"_id": 1, "a": 1, "b": 2}]
            return [{"_id": 1, "a": 1}]

        collection.find.side_effect = find
        opman._catchup_ops = {"k": ("fetch", "test.test", 1, 5)}
        opman._catchup_last_ts = ts(5)
        self.assertTrue(opman._flush_catchup_ops())

        projections = [c[1]["projection"] for c in collection.find.call_args_list]
        self.assertEqual(len(projections), 2)
        self.assertIn(None, projections)
        self.assertIn({"_id": 1, "a": 1}, projections)
        self.assertEqual(
            projected_dm.calls_named("bulk_upsert")[0][1], [{"_id": 1, "a": 1}]
        )
        self.assertEqual(
            full_dm.calls_named("bulk_upsert")[0][1],
            [{"_id": 1, "a": 1, "b": 2}],
        )

    def test_single_query_when_all_doc_managers_agree(self):
        dms = [RecordingDocManager(), RecordingDocManager()]
        for dm in dms:
            dm.catchup_fetch_full_document = True
        opman = self.make_opman(dms)
        collection = self.source_collection(opman)
        collection.find.return_value = [{"_id": 1, "b": 2}]
        opman._catchup_ops = {"k": ("fetch", "test.test", 1, 5)}
        opman._catchup_last_ts = ts(5)
        opman._flush_catchup_ops()
        self.assertEqual(collection.find.call_count, 1)
        self.assertIsNone(collection.find.call_args[1]["projection"])


class TestFlushErrorHandling(OplogThreadTestBase):
    def test_connection_failed_does_not_advance_checkpoint(self):
        dm = RecordingDocManager()
        dm.bulk_upsert_error = errors.ConnectionFailed("down")
        opman = self.make_opman([dm])
        opman.update_checkpoint(ts(1))
        opman._buffer_oplog_entry(insert(2), "test.test", 2)
        with self.assertRaises(CatchupFlushFailed):
            opman._flush_catchup_ops()
        self.assertEqual(opman.checkpoint, ts(1))
        self.assertEqual(opman._catchup_ops, {})
        self.assertTrue(opman._catchup_restart_required)
        # Nothing may be checkpointed or flushed until the oplog is re-read.
        self.assertFalse(opman._advance_checkpoint(ts(3)))
        with self.assertRaises(CatchupFlushFailed):
            opman._flush_catchup_ops()
        self.assertEqual(opman.checkpoint, ts(1))

    def test_serial_fallback_connection_failed_does_not_advance(self):
        dm = RecordingDocManager()
        dm.bulk_apply = mock.Mock(side_effect=ValueError("broken bulk_apply"))
        dm.upsert_errors[2] = errors.ConnectionFailed("down")
        opman = self.make_opman([dm])
        opman.update_checkpoint(ts(1))
        opman._buffer_oplog_entry(insert(2), "test.test", 2)
        with self.assertRaises(CatchupFlushFailed):
            opman._flush_catchup_ops()
        self.assertEqual(opman.checkpoint, ts(1))

    def test_bulk_apply_error_falls_back_to_serial_copies(self):
        dm = RecordingDocManager()
        dm.bulk_apply = mock.Mock(side_effect=ValueError("broken bulk_apply"))
        dm.upsert_errors[3] = errors.OperationFailed("bad doc")
        opman = self.make_opman([dm])
        for n in (2, 3, 4):
            opman._buffer_oplog_entry(insert(n), "test.test", n)
        buffered_doc = opman._catchup_ops[
            next(iter(opman._catchup_ops))
        ][2]
        self.assertTrue(opman._flush_catchup_ops())
        self.assertEqual([c[1]["_id"] for c in dm.calls_named("upsert")], [2, 4])
        self.assertEqual(opman.checkpoint, ts(4))
        self.assertNotIn("_ts", buffered_doc)

    def test_idle_flush_connection_failure_forces_reread(self):
        dm = RecordingDocManager()
        dm.bulk_upsert_error = errors.ConnectionFailed("down")
        opman = self.make_opman([dm], bulk_apply_idle_seconds=1)
        opman._buffer_oplog_entry(insert(2), "test.test", 2)
        opman._catchup_first_buffered_at -= 10
        opman._maybe_flush_idle_buffer()  # must not raise
        self.assertTrue(opman._catchup_restart_required)
        self.assertIsNone(opman.checkpoint)


class TestCheckpointMonotonic(OplogThreadTestBase):
    def setUp(self):
        self.opman = self.make_opman([RecordingDocManager()])

    def test_advance_checkpoint_refuses_to_move_backwards(self):
        self.assertTrue(self.opman._advance_checkpoint(ts(5)))
        self.assertFalse(self.opman._advance_checkpoint(ts(3)))
        self.assertFalse(self.opman._advance_checkpoint(ts(5)))
        self.assertEqual(self.opman.checkpoint, ts(5))
        self.assertTrue(self.opman._advance_checkpoint(ts(6)))
        self.assertEqual(self.opman.checkpoint, ts(6))

    def test_advance_checkpoint_handles_long_checkpoint(self):
        self.opman.checkpoint = bson_ts_to_long(ts(5))
        self.assertFalse(self.opman._advance_checkpoint(ts(4)))
        self.assertTrue(self.opman._advance_checkpoint(ts(6)))

    def test_update_checkpoint_still_allows_rollback(self):
        self.opman.update_checkpoint(ts(5))
        self.opman.update_checkpoint(ts(3))
        self.assertEqual(self.opman.checkpoint, ts(3))


class TestRunLoopCheckpointing(OplogThreadTestBase):
    def test_skipped_entry_does_not_checkpoint_past_buffered_ops(self):
        """Finding 1: buffered op, then a skipped entry, then a cursor
        error. The buffered op was never applied, so the checkpoint must
        not reach the skipped entry's ts."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        written = self.run_with_cursors(
            opman,
            [
                FakeCursor(
                    [insert(1), skipped(2)],
                    error=pymongo.errors.AutoReconnect("lost"),
                )
            ],
        )
        self.assertEqual(dm.calls, [])
        self.assertEqual(written, [])
        self.assertIsNone(opman.checkpoint)

    def test_skipped_entry_checkpointed_after_flush(self):
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        written = self.run_with_cursors(
            opman, [FakeCursor([insert(1), skipped(2)])]
        )
        self.assertEqual(len(dm.calls_named("bulk_upsert")), 1)
        self.assertEqual(written, [ts(2)])

    def test_skipped_apply_ops_sibling_not_checkpointed(self):
        """Sub-ops of an applyOps entry share its ts; a skipped sibling must
        not checkpoint that ts while another sibling is still buffered."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        apply_ops = {
            "ts": ts(5),
            "op": "c",
            "ns": "admin.$cmd",
            "o": {
                "applyOps": [
                    {"op": "i", "ns": "other.coll", "o": {"_id": 1}},
                    {"op": "i", "ns": "test.test", "o": {"_id": 2}},
                ]
            },
        }
        written = self.run_with_cursors(
            opman,
            [FakeCursor([apply_ops], error=pymongo.errors.AutoReconnect("x"))],
        )
        self.assertEqual(written, [])

    def test_checkpoint_does_not_regress_after_flush(self):
        """Finding 2: a skipped entry sets last_ts, then a batch-size flush
        checkpoints a newer ts; the stale last_ts must not be written."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=1)
        written = self.run_with_cursors(
            opman, [FakeCursor([skipped(1), insert(2), insert(3)])]
        )
        self.assertEqual(written, [ts(2), ts(3)])
        self.assertEqual(opman.checkpoint, ts(3))

    def test_connection_failed_rereads_from_checkpoint(self):
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=2)
        opman.update_checkpoint(ts(1))
        original_bulk_upsert = dm.bulk_upsert
        attempts = []

        def flaky_bulk_upsert(docs, namespace, timestamp):
            attempts.append(timestamp)
            if len(attempts) == 1:
                raise errors.ConnectionFailed("down")
            return original_bulk_upsert(docs, namespace, timestamp)

        dm.bulk_upsert = flaky_bulk_upsert
        entries = [insert(2), insert(3)]
        with mock.patch.object(
            OplogThread, "_sleep_while_running"
        ) as backoff:
            written = self.run_with_cursors(
                opman,
                [FakeCursor(list(entries)), FakeCursor(list(entries))],
            )
        backoff.assert_called_once()
        self.assertEqual(len(attempts), 2)
        # Only the successful second flush advanced the checkpoint.
        self.assertEqual(written, [ts(3)])
        self.assertEqual(opman.checkpoint, ts(3))
        self.assertFalse(opman._catchup_restart_required)


def apply_ops(n, sub_ops):
    """An applyOps command entry at ts(n) holding ``sub_ops``."""
    return {
        "ts": ts(n),
        "op": "c",
        "ns": "admin.$cmd",
        "o": {"applyOps": sub_ops},
    }


def sub_insert(doc_id, ns="test.test"):
    return {"op": "i", "ns": ns, "o": {"_id": doc_id}}


def failing_when_present(dm, doc_id):
    """Make dm.bulk_upsert raise ConnectionFailed for any batch holding
    ``doc_id``; returns the list of ids of every batch attempted."""
    original = dm.bulk_upsert
    attempts = []

    def bulk_upsert(docs, namespace, timestamp):
        docs = list(docs)
        attempts.append([d["_id"] for d in docs])
        if any(d["_id"] == doc_id for d in docs):
            raise errors.ConnectionFailed("down")
        return original(iter(docs), namespace, timestamp)

    dm.bulk_upsert = bulk_upsert
    return attempts


class TestApplyOpsAtomicity(OplogThreadTestBase):
    """Sub-ops of an applyOps entry share its ts; no flush may checkpoint
    that ts until every sibling has been applied or buffered."""

    def test_batch_size_flush_deferred_until_entry_processed(self):
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=1)
        written = self.run_with_cursors(
            opman,
            [FakeCursor([apply_ops(5, [sub_insert(1), sub_insert(2)])])],
        )
        # One flush holding both siblings, not one flush per sibling.
        self.assertEqual(
            [[d["_id"] for d in c[1]] for c in dm.calls_named("bulk_upsert")],
            [[1, 2]],
        )
        self.assertEqual(written, [ts(5)])

    def test_failed_flush_does_not_lose_later_siblings(self):
        """With a per-sibling batch-size flush, sibling 1's flush would
        checkpoint ts 5; sibling 2's failed flush would then be lost, since
        the checkpointed entry is not re-read."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=1)
        opman.update_checkpoint(ts(1))
        failing_when_present(dm, 2)
        with mock.patch.object(OplogThread, "_sleep_while_running") as backoff:
            written = self.run_with_cursors(
                opman,
                [FakeCursor([apply_ops(5, [sub_insert(1), sub_insert(2)])])],
            )
        backoff.assert_called_once()
        self.assertEqual(written, [])
        self.assertEqual(opman.checkpoint, ts(1))

    def test_serial_sibling_flush_does_not_checkpoint_shared_ts(self):
        """A serially applied sibling flushes the buffered siblings before
        it, but that flush must not checkpoint the shared ts while a later
        sibling is still to be buffered."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        opman.update_checkpoint(ts(1))
        attempts = failing_when_present(dm, 3)
        entry = apply_ops(5, [sub_insert(1), sub_insert(2), sub_insert(3)])
        with mock.patch.object(OplogThread, "_sleep_while_running"):
            written = self.run_with_cursors(
                opman,
                [FakeCursor([entry])],
                is_behind=lambda e: e["o"]["_id"] != 2,
            )
        self.assertEqual(attempts, [[1], [3]])
        self.assertEqual([c[1]["_id"] for c in dm.calls_named("upsert")], [2])
        self.assertEqual(written, [])
        self.assertEqual(opman.checkpoint, ts(1))

    def test_serial_sibling_checkpointed_after_all_siblings(self):
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        entry = apply_ops(5, [sub_insert(1), sub_insert(2), sub_insert(3)])
        written = self.run_with_cursors(
            opman, [FakeCursor([entry])], is_behind=lambda e: e["o"]["_id"] != 2
        )
        self.assertEqual(
            [c[0] for c in dm.calls if c[0] != "bulk_upsert_input"],
            ["bulk_upsert", "upsert", "bulk_upsert"],
        )
        self.assertEqual(written, [ts(5)])

    def test_serial_entry_flush_still_checkpoints_older_ops(self):
        """Outside an applyOps expansion the pre-serial flush checkpoints
        the buffered ops, which are all older than the serial entry."""
        dm = RecordingDocManager()
        opman = self.make_opman([dm], bulk_apply_batch_size=100)
        written = self.run_with_cursors(
            opman,
            [FakeCursor([insert(1), insert(2)])],
            is_behind=lambda e: e["o"]["_id"] != 2,
        )
        self.assertEqual(written, [ts(1), ts(2)])

    def test_flush_defer_checkpoint_at(self):
        dm = RecordingDocManager()
        opman = self.make_opman([dm])
        opman._buffer_oplog_entry(insert(5), "test.test", 5)
        self.assertFalse(opman._flush_catchup_ops(defer_checkpoint_at=ts(5)))
        self.assertEqual(len(dm.calls_named("bulk_upsert")), 1)
        self.assertEqual(opman._catchup_ops, {})
        self.assertIsNone(opman.checkpoint)
        opman._buffer_oplog_entry(insert(4), "test.test", 4)
        self.assertTrue(opman._flush_catchup_ops(defer_checkpoint_at=ts(5)))
        self.assertEqual(opman.checkpoint, ts(4))

    def test_idle_flush_blocked_during_expansion(self):
        dm = RecordingDocManager()
        opman = self.make_opman(
            [dm], bulk_apply_batch_size=100, bulk_apply_idle_seconds=1
        )
        original_buffer = opman._buffer_oplog_entry
        observed = {}

        def buffer_then_try_idle_flush(entry, ns, timestamp):
            original_buffer(entry, ns, timestamp)
            if entry["o"]["_id"] != 1:
                return
            # The buffer is past its idle deadline after the first sibling;
            # an idle flush from the lag-logger thread must wait for the
            # rest of the entry.
            opman._catchup_first_buffered_at -= 100
            idle = threading.Thread(target=opman._maybe_flush_idle_buffer)
            idle.start()
            idle.join(0.2)
            observed["blocked"] = idle.is_alive()
            observed["flushed"] = len(dm.calls_named("bulk_upsert"))
            observed["thread"] = idle

        opman._buffer_oplog_entry = buffer_then_try_idle_flush
        written = self.run_with_cursors(
            opman,
            [FakeCursor([apply_ops(5, [sub_insert(1), sub_insert(2)])])],
        )
        observed["thread"].join(5)
        self.assertTrue(observed["blocked"])
        self.assertEqual(observed["flushed"], 0)
        self.assertEqual(
            [[d["_id"] for d in c[1]] for c in dm.calls_named("bulk_upsert")],
            [[1, 2]],
        )
        self.assertEqual(written, [ts(5)])


class QuietCursor(object):
    """A live tailable cursor on a quiet source: each pass yields nothing.

    ``on_first_pass`` runs during the first pass; a second pass ends the
    cursor and is recorded, so a test can tell whether the run loop kept
    tailing.
    """

    def __init__(self, on_first_pass, dies_after_first_pass=False):
        self.on_first_pass = on_first_pass
        self.dies_after_first_pass = dies_after_first_pass
        self.alive = True
        self.passes = 0

    def __iter__(self):
        self.passes += 1
        if self.passes == 1:
            self.on_first_pass()
            if self.dies_after_first_pass:
                self.alive = False
        else:
            self.alive = False
        return iter([])


class TestIdleFlushFailureNoticed(OplogThreadTestBase):
    def test_failed_idle_flush_triggers_reread_on_quiet_source(self):
        dm = RecordingDocManager()
        dm.bulk_upsert_error = errors.ConnectionFailed("down")
        opman = self.make_opman([dm], bulk_apply_idle_seconds=1)
        opman.update_checkpoint(ts(1))

        def idle_flush_fails():
            opman._buffer_oplog_entry(insert(2), "test.test", 2)
            opman._catchup_first_buffered_at -= 100
            opman._maybe_flush_idle_buffer()
            self.assertTrue(opman._catchup_restart_required)

        cursor = QuietCursor(idle_flush_fails)
        with mock.patch.object(OplogThread, "_sleep_while_running") as backoff:
            written = self.run_with_cursors(opman, [cursor])
        backoff.assert_called_once()
        self.assertEqual(cursor.passes, 1)
        self.assertEqual(written, [])
        self.assertFalse(opman._catchup_restart_required)

    def test_failed_idle_flush_backs_off_when_cursor_dies(self):
        """The flag is also checked after the cursor loop, so the re-read
        backs off even if the cursor died on the same pass."""
        dm = RecordingDocManager()
        dm.bulk_upsert_error = errors.ConnectionFailed("down")
        opman = self.make_opman([dm], bulk_apply_idle_seconds=1)

        def idle_flush_fails():
            opman._buffer_oplog_entry(insert(2), "test.test", 2)
            opman._catchup_first_buffered_at -= 100
            opman._maybe_flush_idle_buffer()

        cursor = QuietCursor(idle_flush_fails, dies_after_first_pass=True)
        with mock.patch.object(OplogThread, "_sleep_while_running") as backoff:
            written = self.run_with_cursors(opman, [cursor])
        backoff.assert_called_once()
        self.assertEqual(written, [])

    def test_restart_flag_checked_before_tailing(self):
        """A flush that failed on the idle-flush thread after the cursor was
        created is noticed before the cursor is read."""
        opman = self.make_opman([RecordingDocManager()])
        cursor = QuietCursor(lambda: None)
        cursors = [cursor]

        def init_cursor():
            if not cursors:
                opman.running = False
                return None, True
            opman._catchup_restart_required = True
            return cursors.pop(0), False

        opman.init_cursor = init_cursor
        with mock.patch(
            "mongo_connector.oplog_manager.time.sleep"
        ), mock.patch.object(OplogThread, "_sleep_while_running") as backoff:
            opman.run()
        backoff.assert_called_once()
        self.assertEqual(cursor.passes, 0)


class TestReplacementUpdates(OplogThreadTestBase):
    """Replacement-style updates are field-filtered in the oplog entry.
    Doc managers whose serial update() refetches the full source document
    get a full fetch; the others get the filtered payload, with no query."""

    def make(self, full_flags):
        dms = []
        for full in full_flags:
            dm = RecordingDocManager()
            dm.catchup_fetch_full_document = full
            dms.append(dm)
        opman = self.make_opman(
            dms,
            NamespaceConfig(
                namespace_options={"test.test": {"includeFields": ["a"]}}
            ),
        )
        return opman, dms

    def buffer_replacement(self, opman, doc_id=1):
        entry = {
            "ts": ts(5),
            "op": "u",
            "ns": "test.test",
            "o2": {"_id": doc_id},
            "o": {"_id": doc_id, "a": 1, "b": 2},
        }
        skip, _ = opman._should_skip_entry(entry)
        self.assertFalse(skip)
        # filter_oplog_entry trimmed the payload to includeFields.
        self.assertEqual(entry["o"], {"_id": doc_id, "a": 1})
        opman._buffer_oplog_entry(entry, entry["ns"], 5)

    def test_full_document_dm_fetches_unprojected(self):
        opman, (projected_dm, full_dm) = self.make([False, True])
        collection = self.source_collection(opman)
        collection.find.return_value = [{"_id": 1, "a": 1, "b": 2, "c": 3}]
        self.buffer_replacement(opman)
        self.assertTrue(opman._flush_catchup_ops())
        # Only the full-document doc manager needed a query.
        self.assertEqual(collection.find.call_count, 1)
        self.assertIsNone(collection.find.call_args[1]["projection"])
        self.assertEqual(
            projected_dm.calls_named("bulk_upsert")[0][1], [{"_id": 1, "a": 1}]
        )
        self.assertEqual(
            full_dm.calls_named("bulk_upsert")[0][1],
            [{"_id": 1, "a": 1, "b": 2, "c": 3}],
        )
        self.assertEqual(opman.checkpoint, ts(5))

    def test_no_query_without_full_document_dm(self):
        opman, (dm,) = self.make([False])
        collection = self.source_collection(opman)
        self.buffer_replacement(opman)
        self.assertTrue(opman._flush_catchup_ops())
        collection.find.assert_not_called()
        self.assertEqual(dm.calls_named("bulk_upsert")[0][1], [{"_id": 1, "a": 1}])

    def test_full_document_dm_missing_source_doc_becomes_remove(self):
        opman, (dm,) = self.make([True])
        self.source_collection(opman).find.return_value = []
        self.buffer_replacement(opman)
        self.assertTrue(opman._flush_catchup_ops())
        self.assertEqual(dm.calls_named("bulk_upsert"), [])
        self.assertEqual(
            [c[1:3] for c in dm.calls_named("remove")], [(1, "test.test")]
        )

    def test_inserts_keep_filtered_payload(self):
        opman, (dm,) = self.make([True])
        collection = self.source_collection(opman)
        entry = {
            "ts": ts(5),
            "op": "i",
            "ns": "test.test",
            "o": {"_id": 1, "a": 1, "b": 2},
        }
        opman._should_skip_entry(entry)
        opman._buffer_oplog_entry(entry, entry["ns"], 5)
        self.assertTrue(opman._flush_catchup_ops())
        collection.find.assert_not_called()
        self.assertEqual(dm.calls_named("bulk_upsert")[0][1], [{"_id": 1, "a": 1}])

    def test_resolution_never_emits_internal_op_types(self):
        opman, _ = self.make([False])
        self.source_collection(opman).find.return_value = [{"_id": 2}]
        ops = [
            ("replace", "test.test", {"_id": 1, "a": 1}, 1),
            ("fetch", "test.test", 2, 2),
            ("remove", "test.test", 3, 3),
        ]
        for full in (False, True):
            resolved = opman._resolve_catchup_fetches(ops, full_document=full)
            self.assertEqual(
                {op[0] for op in resolved}, {"upsert", "remove"}
            )


class TestReplicationLagLogger(unittest.TestCase):
    def make_opman(self, alive_passes):
        opman = mock.Mock()
        opman.replset_name = "rs0"
        opman.is_alive.side_effect = [True] * alive_passes + [False]
        return opman

    def run_logger(self, opman):
        logger = ReplicationLagLogger(opman, 30)
        with mock.patch("mongo_connector.oplog_manager.time.sleep"):
            logger.run()  # synchronously; returns once is_alive is False

    def test_survives_newest_timestamp_none(self):
        opman = self.make_opman(3)
        opman.checkpoint = ts(5)
        opman.get_last_oplog_timestamp.return_value = None  # TypeError
        self.run_logger(opman)
        self.assertEqual(opman._maybe_flush_idle_buffer.call_count, 3)

    def test_survives_retry_exhaustion_and_flush_errors(self):
        opman = self.make_opman(2)
        opman.checkpoint = ts(5)
        opman._maybe_flush_idle_buffer.side_effect = [RuntimeError("x"), None]
        with mock.patch(
            "mongo_connector.oplog_manager.retry_until_ok",
            side_effect=pymongo.errors.AutoReconnect("gave up"),
        ):
            self.run_logger(opman)
        self.assertEqual(opman._maybe_flush_idle_buffer.call_count, 2)


class TestCatchupHysteresis(OplogThreadTestBase):
    """Bulk mode is entered above bulkApplyLagSeconds and left at the exit lag."""

    NEWEST = 10000

    def make(self, **kwargs):
        opman = self.make_opman([RecordingDocManager()], **kwargs)
        opman._newest_oplog_ts = bson.Timestamp(self.NEWEST, 1)
        # keep the cached newest ts: never refresh from the (mock) source
        opman._entries_since_lag_check = -(10 ** 9)
        return opman

    def behind(self, opman, lag):
        return opman._is_behind_oplog({"ts": bson.Timestamp(self.NEWEST - lag, 1)})

    def test_defaults(self):
        opman = self.make_opman([RecordingDocManager()])
        self.assertEqual(opman.bulk_apply_lag_seconds, 300)
        self.assertEqual(opman.bulk_apply_exit_lag_seconds, 60)

    def test_enters_only_above_entry_lag(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        self.assertFalse(self.behind(opman, 300))
        self.assertFalse(opman._catchup_mode_active)
        self.assertTrue(self.behind(opman, 301))
        self.assertTrue(opman._catchup_mode_active)

    def test_stays_active_until_exit_lag(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        self.assertTrue(self.behind(opman, 500))
        for lag in (299, 200, 100, 61):
            self.assertTrue(self.behind(opman, lag), lag)
        self.assertFalse(self.behind(opman, 60))
        self.assertFalse(opman._catchup_mode_active)

    def test_does_not_reenter_below_entry_lag(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        self.assertTrue(self.behind(opman, 400))
        self.assertFalse(self.behind(opman, 30))
        self.assertFalse(self.behind(opman, 200))
        self.assertTrue(self.behind(opman, 301))

    def test_transitions_logged_once(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        with mock.patch("mongo_connector.oplog_manager.LOG") as log:
            for lag in (400, 350, 200, 70, 30, 20, 400):
                self.behind(opman, lag)
        messages = [c.args[1] for c in log.info.call_args_list]
        self.assertEqual(messages, ["entering", "leaving", "entering"])

    def test_run_loop_stays_in_bulk_until_exit_lag(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        dm = opman.doc_managers[0]
        lags = [400, 250, 100, 61, 60, 10]
        entries = [
            {
                "op": "i",
                "ns": "test.coll",
                "o": {"_id": i},
                "ts": bson.Timestamp(self.NEWEST - lag, 1),
            }
            for i, lag in enumerate(lags)
        ]
        self.run_with_cursors(
            opman, [FakeCursor(entries)], is_behind=opman._is_behind_oplog
        )
        kinds = [c[0] for c in dm.calls if c[0] in ("bulk_upsert", "upsert")]
        bulk_ids = [
            d["_id"] for c in dm.calls if c[0] == "bulk_upsert" for d in c[1]
        ]
        serial_ids = [c[1]["_id"] for c in dm.calls if c[0] == "upsert"]
        self.assertEqual(bulk_ids, [0, 1, 2, 3])
        self.assertEqual(serial_ids, [4, 5])
        self.assertLess(kinds.index("bulk_upsert"), kinds.index("upsert"))

    def test_run_loop_cold_start_below_entry_lag_is_serial(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        dm = opman.doc_managers[0]
        entry = {
            "op": "i",
            "ns": "test.coll",
            "o": {"_id": 1},
            "ts": bson.Timestamp(self.NEWEST - 250, 1),
        }
        self.run_with_cursors(
            opman, [FakeCursor([entry])], is_behind=opman._is_behind_oplog
        )
        self.assertEqual([c[0] for c in dm.calls if c[0] == "bulk_upsert"], [])
        self.assertEqual([c[1]["_id"] for c in dm.calls if c[0] == "upsert"], [1])

    def test_exit_above_entry_is_clamped_with_warning(self):
        with mock.patch("mongo_connector.oplog_manager.LOG") as log:
            opman = self.make(
                bulk_apply_lag_seconds=100, bulk_apply_exit_lag_seconds=500
            )
        self.assertEqual(opman.bulk_apply_exit_lag_seconds, 100)
        self.assertTrue(log.warning.called)
        # clamped exit == entry: single-threshold behavior
        self.assertTrue(self.behind(opman, 101))
        self.assertFalse(self.behind(opman, 100))

    def test_exit_equal_to_entry_is_single_threshold(self):
        with mock.patch("mongo_connector.oplog_manager.LOG") as log:
            opman = self.make(
                bulk_apply_lag_seconds=100, bulk_apply_exit_lag_seconds=100
            )
        self.assertFalse(log.warning.called)
        self.assertTrue(self.behind(opman, 101))
        self.assertFalse(self.behind(opman, 100))

    def test_exit_zero_stays_until_fully_caught_up(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=0)
        self.assertTrue(self.behind(opman, 400))
        self.assertTrue(self.behind(opman, 1))
        self.assertFalse(self.behind(opman, 0))

    def test_disabled_feature_ignores_exit_lag(self):
        opman = self.make(bulk_apply_lag_seconds=0, bulk_apply_exit_lag_seconds=60)
        self.assertFalse(opman._bulk_catchup_enabled())

    def test_unknown_newest_keeps_current_mode(self):
        opman = self.make(bulk_apply_lag_seconds=300, bulk_apply_exit_lag_seconds=60)
        self.assertTrue(self.behind(opman, 400))
        opman._newest_oplog_ts = None
        opman.get_last_oplog_timestamp = lambda: None
        opman._entries_since_lag_check = 0
        self.assertTrue(self.behind(opman, 10))


class TestConfigValidation(unittest.TestCase):
    def load(self, d):
        import json

        from mongo_connector import config
        from mongo_connector.connector import get_config_options

        conf = config.Config(get_config_options())
        conf.load_json(json.dumps(d))
        conf.parse_args([])
        return conf

    def test_valid_values(self):
        conf = self.load(
            {
                "bulkApplyLagSeconds": 0,
                "bulkApplyBatchSize": 1,
                "bulkApplyIdleSeconds": 0,
                "bulkApplyExitLagSeconds": 0,
            }
        )
        self.assertEqual(conf["bulkApplyBatchSize"], 1)
        self.assertEqual(conf["bulkApplyExitLagSeconds"], 0)

    def test_exit_lag_default(self):
        from mongo_connector import constants

        conf = self.load({})
        self.assertEqual(
            conf["bulkApplyExitLagSeconds"],
            constants.DEFAULT_BULK_APPLY_EXIT_LAG_SECONDS,
        )
        self.assertEqual(conf["bulkApplyExitLagSeconds"], 60)

    def test_invalid_values(self):
        for d in (
            {"bulkApplyBatchSize": 0},
            {"bulkApplyLagSeconds": -1},
            {"bulkApplyIdleSeconds": -1},
            {"bulkApplyExitLagSeconds": -1},
        ):
            with self.assertRaises(errors.InvalidConfiguration):
                self.load(d)

    def test_invalid_cli_values(self):
        from mongo_connector import config
        from mongo_connector.connector import get_config_options

        conf = config.Config(get_config_options())
        with self.assertRaises(errors.InvalidConfiguration):
            conf.parse_args(["--bulk-apply-batch-size", "0"])
        conf = config.Config(get_config_options())
        with self.assertRaises(errors.InvalidConfiguration):
            conf.parse_args(["--bulk-apply-exit-lag-seconds", "-1"])
        conf = config.Config(get_config_options())
        conf.parse_args(["--bulk-apply-exit-lag-seconds", "0"])
        self.assertEqual(conf["bulkApplyExitLagSeconds"], 0)


if __name__ == "__main__":
    unittest.main()
