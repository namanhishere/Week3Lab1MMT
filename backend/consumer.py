"""
Backend consumer — reads messages from RabbitMQ and displays them.
Demonstrates asynchronous backend processing decoupled from device communication.
"""
import json
import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pika
import config


class SensorDataConsumer:
    """Consumes sensor data from RabbitMQ and processes it."""

    def __init__(self):
        self.messages_received = 0
        self.protocol_counts = {"HTTP": 0, "MQTT": 0, "CoAP": 0}
        self.alerts = []
        self.start_time = time.time()

    def callback(self, channel, method, properties, body):
        """Called when a message arrives from RabbitMQ."""
        try:
            data = json.loads(body.decode())
            self.messages_received += 1
            protocol = data.get("protocol", "unknown")
            self.protocol_counts[protocol] = self.protocol_counts.get(protocol, 0) + 1

            # Check if it's an alert
            is_alert = data.get("alert", False)
            alert_type = data.get("alert_type", "")

            elapsed = time.time() - self.start_time
            marker = "⚠️  ALERT" if is_alert else "📊 TELEMETRY"

            print(f"\n{'='*70}")
            print(f"  [{marker}] Message #{self.messages_received} (t+{elapsed:.1f}s)")
            print(f"  Protocol: {protocol}")
            print(f"  Device:   {data.get('device_id', 'N/A')}")

            if is_alert:
                self.alerts.append(data)
                print(f"  Alert:    {alert_type}")
                print(f"  Message:  {data.get('message', 'N/A')}")

            # Show sensor values if present
            for field in ["temperature", "humidity", "soil_moisture"]:
                if field in data:
                    print(f"  {field}: {data[field]}")

            print(f"  Routing:  {method.routing_key}")
            print(f"  Queue:    {method.routing_key} → {config.RABBITMQ_QUEUE}")
            print(f"{'='*70}")

            # Acknowledge the message
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to decode message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def print_summary(self):
        """Print a summary of all received messages."""
        elapsed = time.time() - self.start_time
        print(f"\n{'#'*70}")
        print(f"  CONSUMPTION SUMMARY")
        print(f"{'#'*70}")
        print(f"  Total messages:     {self.messages_received}")
        print(f"  Running time:       {elapsed:.1f}s")
        print(f"  Rate:               {self.messages_received/max(elapsed,0.1):.1f} msg/s")
        print(f"  --- By Protocol ---")
        for proto, count in self.protocol_counts.items():
            if count > 0:
                print(f"    {proto}: {count} messages")
        print(f"  --- Alerts ---")
        if self.alerts:
            for a in self.alerts:
                print(f"    [{a.get('protocol')}] {a.get('alert_type')}: {a.get('message', '')[:50]}")
        else:
            print(f"    No alerts received")
        print(f"{'#'*70}")

    def run(self):
        """Start consuming from RabbitMQ."""
        print("=" * 70)
        print("  IoT BACKEND CONSUMER — RabbitMQ")
        print("=" * 70)
        print(f"  Exchange:  {config.RABBITMQ_EXCHANGE} ({config.RABBITMQ_EXCHANGE_TYPE})")
        print(f"  Queue:     {config.RABBITMQ_QUEUE}")
        print(f"  Binding:   {config.RABBITMQ_ROUTING_KEY}")
        print(f"  Broker:    {config.RABBITMQ_HOST}:{config.RABBITMQ_PORT}")
        print("=" * 70)
        print("  Waiting for messages... (Ctrl+C to stop and show summary)\n")

        credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            credentials=credentials,
        )

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Ensure queue exists
        channel.queue_declare(queue=config.RABBITMQ_QUEUE, durable=True)

        # Fair dispatch — don't give more than 1 unacked message at a time
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=config.RABBITMQ_QUEUE,
            on_message_callback=self.callback,
            auto_ack=False,
        )

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print("\n\nStopping consumer...")
            channel.stop_consuming()
            self.print_summary()
        finally:
            connection.close()


if __name__ == "__main__":
    consumer = SensorDataConsumer()
    consumer.run()
