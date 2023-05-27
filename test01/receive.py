import pika, sys, os


def main():
    # 1。 与RabbitMQ服务器建立连接。
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # 2。 下一步，就像发布消息一样，是确保队列存在。使用queue_declare创建队列是幂等的——我们可以根据需要多次运行该命令，并且只会创建一次。
    channel.queue_declare(queue='hello')

    # 3。 从队列接收消息更为复杂。它通过将回调函数订阅到队列来工作。每当我们收到一条消息时，这个回调函数就会被 Pika 库调用。在我们的例子中，
    # 这个函数将在屏幕上打印消息的内容
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

    # 4。 接下来，我们需要告诉 RabbitMQ 这个特定的回调函数应该从我们的hello队列接收消息（为了使该命令成功，我们必须确保我们要订阅的队列存在。
    # 幸运的是，我们对此充满信心——我们已经在上面使用queue_declare创建了一个队列—。）：
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    # 5。 最后，我们进入一个永无止境的循环，等待数据并在必要时运行回调，并在程序关闭期间捕获KeyboardInterrupt。
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
