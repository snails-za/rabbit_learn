import pika
import sys

# 1。 与RabbitMQ服务器建立连接
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2。 创建交换机
channel.exchange_declare(exchange='logs', exchange_type='fanout')

# 3。发布信息到logs交换机
message = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(exchange='logs', routing_key='', body=message)
print(" [x] Sent %r" % message)
connection.close()
