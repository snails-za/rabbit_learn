import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection, DeliveryMode, AbstractIncomingMessage
from aio_pika.pool import Pool


async def consume(channel_pool, queue_name) -> None:
    async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
        await channel.set_qos(10)

        queue = await channel.declare_queue(
            queue_name, durable=True, auto_delete=False,
        )
        await on_message(queue)


async def publish(channel_pool, queue_name, message_body) -> None:
    async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
        await channel.default_exchange.publish(
            aio_pika.Message((message_body).encode(), delivery_mode=DeliveryMode.PERSISTENT),
            queue_name,
        )


async def get_channel_pool(loop, connection_pool) -> aio_pika.pool.Pool:
    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=20, loop=loop)
    return channel_pool


async def get_connection() -> AbstractRobustConnection:
    return await aio_pika.connect_robust("amqp://guest:guest@localhost/")


async def on_message(queue) -> None:
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            print(message.body)
            await message.ack()


async def main():
    loop = asyncio.get_event_loop()
    connection_pool = Pool(get_connection, max_size=20, loop=loop)
    channel_pool = await get_channel_pool(loop, connection_pool)
    async with connection_pool, channel_pool:
        task = loop.create_task(consume(channel_pool, "raw_data"))
        await task

if __name__ == '__main__':
    asyncio.run(main())








