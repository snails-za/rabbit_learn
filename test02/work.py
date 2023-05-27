import pika
import time

# 1。 与RabbitMQ服务器建立连接。
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 2。 下一步，就像发布消息一样，是确保队列存在。使用queue_declare创建队列是幂等的——我们可以根据需要多次运行该命令，并且只会创建一次。
# 尽管这行代码本身是正确的，但是仍然不会正确运行。因为我们已经定义过一个叫hello的非持久化队列。RabbitMq不允许你使用不同的参数重新
# 定义一个队列，它会返回一个错误。但我们现在使用一个快捷的解决方法——用不同的名字，例如task_queue。
# channel.queue_declare(queue='hello', durable=True)
channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


# 3。 从队列接收消息更为复杂。它通过将回调函数订阅到队列来工作。每当我们收到一条消息时，这个回调函数就会被 Pika 库调用。在我们的例子中，
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    # 一个很容易犯的错误就是忘了basic_ack，后果很严重。消息在你的程序退出之后就会重新发送，如果它不能够释放没响应的消息，RabbitMQ就会
    # 占用越来越多的内存。
    # 为了排除这种错误，你可以使用rabbitmqctl命令，输出messages_unacknowledged字段：
    # rabbitmqctl list_queues name messages_ready messages_unacknowledged
    ch.basic_ack(delivery_tag=method.delivery_tag)


# 4。 接下来，我们需要告诉 RabbitMQ 这个特定的回调函数应该从我们的hello队列接收消息（为了使该命令成功，我们必须确保我们要订阅的队列存在。
# 幸运的是，我们对此充满信心——我们已经在上面创建了一个队列——使用queue_declare。）：
# 我们可以使用basic.qos方法，并设置prefetch_count=1。这样是告诉RabbitMQ，再同一时刻，不要发送超过1条消息给一个工作者（worker），直
# 到它已经处理了上一条消息并且作出了响应。这样，RabbitMQ就会把消息分发给下一个空闲的工作者（worker）。
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()
