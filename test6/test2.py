import asyncio

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage


RABBITMQ_CONFIG = {
    "username": "guest",
    "password": "guest",
    "host": "127.0.0.1",
    "port": 5672
}


async def consume(queuename, func) -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )
    async with connection:
        # Creating a channel
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        # Declaring queue
        queue = await channel.declare_queue(
            queuename,
            durable=True,
        )

        # Start listening the queue with name 'task_queue'
        await queue.consume(func)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] Received message {message!r}")
        print(f"     Message body is: {message.body!r}")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    max_count = 5
    tasks = [consume("raw_data", on_message) for _ in range(max_count)]
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


