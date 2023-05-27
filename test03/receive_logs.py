import pika

# 1。 与RabbitMQ服务器建立连接
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2。创建交换机
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# 3。创建临时队列
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# 4。交换机绑定临时队列
channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] %r" % body)


# channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()