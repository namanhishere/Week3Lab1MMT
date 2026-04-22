"""
RabbitMQ setup utility — creates the exchange, queue, and binding.
Called by gateway on startup.
"""
import pika
import config


def setup_rabbitmq():
    """Declare exchange, queue, and binding for the IoT pipeline."""
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declare a topic exchange
    channel.exchange_declare(
        exchange=config.RABBITMQ_EXCHANGE,
        exchange_type=config.RABBITMQ_EXCHANGE_TYPE,
        durable=True,
    )

    # Declare a queue
    channel.queue_declare(queue=config.RABBITMQ_QUEUE, durable=True)

    # Bind queue to exchange with routing key pattern
    channel.queue_bind(
        queue=config.RABBITMQ_QUEUE,
        exchange=config.RABBITMQ_EXCHANGE,
        routing_key=config.RABBITMQ_ROUTING_KEY,
    )

    print(f"[setup] Exchange '{config.RABBITMQ_EXCHANGE}' declared")
    print(f"[setup] Queue '{config.RABBITMQ_QUEUE}' declared")
    print(f"[setup] Binding '{config.RABBITMQ_ROUTING_KEY}' → '{config.RABBITMQ_QUEUE}'")
    connection.close()
    return True


def get_connection():
    """Return a blocking channel ready for publishing."""
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    return connection, channel


if __name__ == "__main__":
    setup_rabbitmq()
