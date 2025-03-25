from contextlib import contextmanager
from typing import Callable, ContextManager, Optional, Set
from unittest.mock import patch
from uuid import UUID

from bson import CodecOptions, UuidRepresentation
from bson.raw_bson import RawBSONDocument
from pymongo import InsertOne, UpdateOne, WriteConcern
from pymongo.errors import AutoReconnect, ConnectionFailure

from tests.utils import SingleCollectionTest, catch_sent_msgs
from txmongo import MongoProtocol
from txmongo.collection import Collection
from txmongo.sessions import ClientSession


async def iterate_find_with_cursor(coll: Collection, session: Optional[ClientSession]):
    batch, dfr = await coll.find_with_cursor(session=session)
    while batch:
        batch, dfr = await dfr


class TestClientSessions(SingleCollectionTest):
    @contextmanager
    def _test_has_no_lsid(self):
        with catch_sent_msgs() as messages:
            yield

        self.assertGreater(len(messages), 0)
        for msg in messages:
            self.assertNotIn("lsid", msg.to_dict())

    @contextmanager
    def _test_has_lsid(
        self, session_id: RawBSONDocument = None
    ) -> ContextManager[Callable[[], Set[UUID]]]:
        session_ids = set()
        with catch_sent_msgs() as messages:
            yield lambda: session_ids

        self.assertGreater(len(messages), 0)
        for msg in messages:
            lsid = msg.to_dict(
                CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
            )["lsid"]
            session_ids.add(lsid["id"])
            if session_id is None:
                self.assertIsInstance(lsid["id"], UUID)
            else:
                self.assertEqual(lsid["id"], session_id["id"])

    coll_reads = [
        lambda coll, session: coll.distinct("x", session=session),
        lambda coll, session: coll.aggregate([{"$count": "cnt"}], session=session),
        lambda coll, session: coll.map_reduce(
            "function () { emit(this.src, 1) }",
            "function (key, values) { return Array.sum(values) }",
            out={"inline": 1},
            session=session,
        ),
        lambda coll, session: coll.find({}, session=session),
        lambda coll, session: coll.find_one({}, session=session),
        lambda coll, session: iterate_find_with_cursor(coll, session),
        lambda coll, session: coll.count_documents({}, session=session),
    ]

    coll_writes = [
        lambda coll, session: coll.insert_one({}, session=session),
        lambda coll, session: coll.insert_many(
            [{} for _ in range(10)], session=session
        ),
        lambda coll, session: coll.update_one({}, {"$set": {"x": 2}}, session=session),
        lambda coll, session: coll.update_many({}, {"$set": {"x": 2}}, session=session),
        lambda coll, session: coll.replace_one({}, {}, session=session),
        lambda coll, session: coll.delete_one({}, session=session),
        lambda coll, session: coll.delete_many({}, session=session),
        lambda coll, session: coll.bulk_write(
            [InsertOne({}), UpdateOne({}, {"$set": {"x": 2}})],
            session=session,
        ),
        lambda coll, session: coll.find_one_and_delete({}, session=session),
        lambda coll, session: coll.find_one_and_update(
            {}, {"$set": {"y": 1}}, session=session
        ),
        lambda coll, session: coll.find_one_and_replace({}, {}, session=session),
    ]

    coll_operations = coll_reads + coll_writes

    db_operations = [
        lambda db, session: db.command("ismaster", session=session),
    ]

    async def test_explicit_session_id(self):
        session = self.db.connection.start_session()
        for op in self.coll_operations:
            with self._test_has_lsid(session.session_id):
                await op(self.coll, session)
        for op in self.db_operations:
            with self._test_has_lsid(session.session_id):
                await op(self.db, session)
        await session.end_session()

    async def test_session_reuse(self):
        """Session ID is reused after end_session()"""
        s = self.db.connection.start_session()
        id1 = s.session_id
        await s.end_session()
        s = self.db.connection.start_session()
        id2 = s.session_id
        await s.end_session()
        self.assertEqual(id1, id2)

    async def test_id_of_ended_session_raises(self):
        s = self.db.connection.start_session()
        self.assertFalse(s.is_ended)
        await s.end_session()
        self.assertTrue(s.is_ended)
        with self.assertRaises(ValueError):
            _ = s.session_id

    async def test_session_cache_is_lifo(self):
        """Session ID cache is LIFO"""
        s1 = self.db.connection.start_session()
        id1 = s1.session_id
        s2 = self.db.connection.start_session()
        id2 = s2.session_id
        await s1.end_session()
        await s2.end_session()

        s3 = self.db.connection.start_session()
        id3 = s3.session_id
        s4 = self.db.connection.start_session()
        id4 = s4.session_id
        await s3.end_session()
        await s4.end_session()

        self.assertEqual(id3, id2)
        self.assertEqual(id4, id1)

    async def test_implicit_session(self):
        """Implicit session ID is used (and reused)"""
        session_ids = set()

        # adding the first operation again to test session reusing by the last operation
        for op in self.coll_operations + [self.coll_operations[0]]:
            with self._test_has_lsid() as get_session_ids:
                await op(self.coll, None)
            session_ids.update(get_session_ids())
        for op in self.db_operations + [self.db_operations[0]]:
            with self._test_has_lsid() as get_session_ids:
                await op(self.db, None)
            session_ids.update(get_session_ids())

        # All ops should share the same implicit session if each of them properly
        # releases its session
        self.assertEqual(len(session_ids), 1)

    async def test_with_cursor(self):
        await self.coll.insert_many({"x": i} for i in range(1, 11))

        cursor = self.coll.find().batch_size(1)
        async for _ in cursor:
            # is_ended is false even for the last batch, because the cursor is not exhausted
            # until we do 11th call to DB
            self.assertFalse(cursor.session.is_ended)
        self.assertTrue(cursor.session.is_ended)

        cursor = self.coll.find().sort({"x": 1}).batch_size(4)
        async for doc in cursor:
            # session is closed immediately after we get a final batch
            self.assertEqual(cursor.session.is_ended, doc["x"] >= 9)

        # session is closed after `break`
        cursor = self.coll.find().sort({"x": 1}).batch_size(1)
        async for doc in cursor:
            self.assertFalse(cursor.session.is_ended)
            if doc["x"] == 3:
                break
        self.assertTrue(cursor.session.is_ended)

        # session is closed when cursor is explicitly closed
        cursor = self.coll.find().batch_size(1)
        await cursor.next_batch()
        await cursor.next_batch()
        self.assertFalse(cursor.session.is_ended)
        cursor.close()
        self.assertTrue(cursor.session.is_ended)

    async def test_unacknowledged_no_implicit_session(self):
        """Test that implicit session is not used with unacknowledged writes"""

        coll_unack = self.coll.with_options(write_concern=WriteConcern(w=0))

        for op in self.coll_writes:
            with self._test_has_no_lsid():
                await op(coll_unack, None)

    async def test_unacknowledged_no_explicit_session(self):
        """Test that explicit session cannot be used with unacknowledged writes"""
        coll_unack = self.coll.with_options(write_concern=WriteConcern(w=0))

        for op in self.coll_writes:
            with self.assertRaises(ValueError):
                await op(coll_unack, self.db.connection.start_session())

    async def test_discard_dirty_session(self):
        """Test that dirty session is discarded"""

        session = self.db.connection.start_session()
        session_id = session.session_id
        await self.coll.insert_one({"_id": 1}, session=session)
        with self.assertRaises(ConnectionFailure):
            with patch.object(
                MongoProtocol,
                "_send_raw_msg",
                side_effect=AutoReconnect(),
                autospec=True,
            ):
                await self.coll.insert_one({"_id": 2}, session=session)
        await session.end_session()

        new_session = self.db.connection.start_session()
        self.assertNotEqual(session_id, new_session.session_id)
        await new_session.end_session()
