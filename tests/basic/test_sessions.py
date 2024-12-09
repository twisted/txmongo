from contextlib import contextmanager
from typing import Callable, ContextManager, Optional, Set
from unittest.mock import patch
from uuid import UUID

import bson
from bson import CodecOptions, UuidRepresentation
from bson.raw_bson import RawBSONDocument
from pymongo import InsertOne, UpdateOne

from tests.utils import SingleCollectionTest, patch_send_msg
from txmongo import MongoProtocol
from txmongo.sessions import ClientSession


class TestClientSessions(SingleCollectionTest):
    @contextmanager
    def _test_has_lsid(
        self, session_id: RawBSONDocument = None
    ) -> ContextManager[Callable[[], Set[UUID]]]:
        with patch_send_msg() as mock:
            session_ids = set()

            yield lambda: session_ids

            mock.assert_called()
            for call in mock.call_args_list:
                msg = call.args[1]
                cmd = bson.decode(
                    msg.body,
                    codec_options=CodecOptions(
                        uuid_representation=UuidRepresentation.STANDARD
                    ),
                )
                lsid = cmd["lsid"]
                session_ids.add(lsid["id"])
                if session_id is None:
                    self.assertIsInstance(lsid["id"], UUID)
                else:
                    self.assertEqual(lsid["id"], session_id["id"])

    async def _iterate_find_with_cursor(self, session: Optional[ClientSession]):
        batch, dfr = await self.coll.find_with_cursor(session=session)
        while batch:
            batch, dfr = await dfr

    operations = [
        lambda self, session: self.db.command("ismaster", session=session),
        lambda self, session: self.coll.insert_one({}, session=session),
        lambda self, session: self.coll.insert_many(
            [{} for _ in range(10)], session=session
        ),
        lambda self, session: self.coll.update_one(
            {}, {"$set": {"x": 2}}, session=session
        ),
        lambda self, session: self.coll.update_many(
            {}, {"$set": {"x": 2}}, session=session
        ),
        lambda self, session: self.coll.replace_one({}, {}, session=session),
        lambda self, session: self.coll.delete_one({}, session=session),
        lambda self, session: self.coll.delete_many({}, session=session),
        lambda self, session: self.coll.bulk_write(
            [InsertOne({}), UpdateOne({}, {"$set": {"x": 2}})],
            session=session,
        ),
        lambda self, session: self.coll.distinct("x", session=session),
        lambda self, session: self.coll.aggregate([{"$count": "cnt"}], session=session),
        lambda self, session: self.coll.map_reduce(
            "function () { emit(this.src, 1) }",
            "function (key, values) { return Array.sum(values) }",
            out={"inline": 1},
            session=session,
        ),
        lambda self, session: self.coll.find_one_and_delete({}, session=session),
        lambda self, session: self.coll.find_one_and_update(
            {}, {"$set": {"y": 1}}, session=session
        ),
        lambda self, session: self.coll.find_one_and_replace({}, {}, session=session),
        lambda self, session: self.coll.find({}, session=session),
        lambda self, session: self.coll.find_one({}, session=session),
        lambda self, session: self._iterate_find_with_cursor(session),
    ]

    async def test_explicit_session_id(self):
        session = self.db.connection.start_session()
        for op in self.operations:
            with self._test_has_lsid(session.session_id):
                await op(self, session)
        session.end_session()

    def test_session_reuse(self):
        """Session ID is reused after end_session()"""
        s = self.db.connection.start_session()
        id1 = s.session_id
        s.end_session()
        s = self.db.connection.start_session()
        id2 = s.session_id
        s.end_session()
        self.assertEqual(id1, id2)

    def test_id_of_ended_session_raises(self):
        s = self.db.connection.start_session()
        self.assertFalse(s.is_ended)
        s.end_session()
        self.assertTrue(s.is_ended)
        with self.assertRaises(ValueError):
            _ = s.session_id

    def test_session_cache_is_lifo(self):
        """Session ID cache is LIFO"""
        s1 = self.db.connection.start_session()
        id1 = s1.session_id
        s2 = self.db.connection.start_session()
        id2 = s2.session_id
        s1.end_session()
        s2.end_session()

        s3 = self.db.connection.start_session()
        id3 = s3.session_id
        s4 = self.db.connection.start_session()
        id4 = s4.session_id
        s3.end_session()
        s4.end_session()

        self.assertEqual(id3, id2)
        self.assertEqual(id4, id1)

    async def test_implicit_session(self):
        """Implicit session ID is used (and reused)"""
        session_ids = set()

        # adding the first operation again to test session reusing by the last operation
        for op in self.operations + [self.operations[0]]:
            with self._test_has_lsid() as get_session_ids:
                await op(self, None)
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
