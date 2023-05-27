import pika
import sys

# 1。 与RabbitMQ服务器建立连接。
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2。 创建队列
# 尽管这行代码本身是正确的，但是仍然不会正确运行。因为我们已经定义过一个叫hello的非持久化队列。RabbitMq不允许你使用不同的参数重新
# 定义一个队列，它会返回一个错误。但我们现在使用一个快捷的解决方法——用不同的名字，例如task_queue。
# channel.queue_declare(queue='hello', durable=True)
channel.queue_declare(queue='task_queue', durable=True)

# 3。 使用由空字符串标识的默认交换，这个交换很特别————允许我们准确地指定消息应该去哪个队列。需要在routing_key参数中指定队列名称：
message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))
print(" [x] Sent %r" % message)

# 4。 在退出程序之前，我们需要确保网络缓冲区已刷新并且我们的消息实际上已传递到 RabbitMQ。我们可以通过轻轻关闭连接来实现。
connection.close()
