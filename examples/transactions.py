from pymongo import UpdateOne
from twisted.internet.task import react

from txmongo.connection import ConnectionPool


@react
async def main(reactor):
    conn = ConnectionPool()
    db = conn.db

    # Using context manager syntax
    async with conn.start_session() as session:
        # All operations in this block will be either executed or rolled back at once in case of error in any of them
        async with session.start_transaction():
            order_id = 12345
            order = await db.orders.find_one({"order_id": order_id}, session=session)
            await db.orders.update_one(
                {"order_id": order_id}, {"$set": {"status": "paid"}}, session=session
            )
            await db.stock.bulk_write(
                [
                    UpdateOne(
                        {"item_id": item["item_id"]},
                        {"$inc": {"stock": -item["quantity"]}},
                    )
                    for item in order["items"]
                ],
                session=session,
            )

    # Manual error handling
    session = conn.start_session()
    session.start_transaction()
    order_id = 12345
    try:
        order = await db.orders.find_one({"order_id": order_id}, session=session)
        await db.orders.update_one(
            {"order_id": order_id}, {"$set": {"status": "paid"}}, session=session
        )
        await db.stock.bulk_write(
            [
                UpdateOne(
                    {"item_id": item["item_id"]},
                    {"$inc": {"stock": -item["quantity"]}},
                )
                for item in order["items"]
            ],
            session=session,
        )
    except Exception:
        await session.abort_transaction()
    else:
        await session.commit_transaction()
    await session.end_session()
