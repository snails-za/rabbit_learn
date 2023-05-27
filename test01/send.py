import pika

# 1。 与RabbitMQ服务器建立连接。
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2。 创建队列
channel.queue_declare(queue='hello')

# 3。 使用由空字符串标识的默认交换，这个交换很特别————允许我们准确地指定消息应该去哪个队列。需要在routing_key参数中指定队列名称：
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print(" [x] Sent 'Hello World!'")

# 4。 在退出程序之前，我们需要确保网络缓冲区已刷新并且我们的消息实际上已传递到 RabbitMQ。我们可以通过轻轻关闭连接来实现。
connection.close()
