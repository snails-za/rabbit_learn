import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection, DeliveryMode, AbstractIncomingMessage
from aio_pika.pool import Pool

RABBITMQ_CONFIG = {
    "username": "guest",
    "password": "guest",
    "host": "127.0.0.1",
    "port": 5672
}


async def consume(channel_pool, queue_name, func) -> None:
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
    data = [
        """<212>Oct 29 11:50:51 ips IPS: SerialNum=0113201403099999 GenTime="2022-03-29 11:50:51" SrcIP=192.168.38.28 SrcIP6= SrcIPVer=4 DstIP=82.244.22.168 DstIP6= DstIPVer=4 Protocol=UDP SrcPort=58380 DstPort=16464 InInterface=ge0/3 OutInterface=ge0/4 SMAC=00:0c:29:bb:28:60 DMAC=00:17:df:ba:4c:00 FwPolicyID=6 EventName=UDP_Trojan.Malagent_连接 EventID=152340118 EventLevel=2 EventsetName=all SecurityType=木马后门 SecurityID=5 ProtocolType=UDP ProtocolID=5 Action=PASS Vsysid=0 Content="" CapToken= EvtCount=1""",
    ]

    loop = asyncio.get_event_loop()
    connection_pool = Pool(get_connection, max_size=20, loop=loop)
    channel_pool = await get_channel_pool(loop, connection_pool)
    async with connection_pool, channel_pool:
        await asyncio.wait([publish(channel_pool, "raw_data", data[0]) for _ in range(50)])

if __name__ == '__main__':
    asyncio.run(main())
