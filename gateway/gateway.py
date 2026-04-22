"""
Gateway service — accepts data from HTTP, MQTT, and CoAP device simulators,
normalizes the payload, and publishes into RabbitMQ.
"""
import json
import time
import asyncio
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pika
import aiohttp
from aiohttp import web
import aiocoap
import aiocoap.resource as resource
from aiocoap import Message, Code

import config
from rabbitmq_setup import setup_rabbitmq, get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# RabbitMQ publisher (shared)
# ---------------------------------------------------------------------------
_rmq_conn = None
_rmq_ch = None


def publish_to_rabbitmq(routing_key: str, payload: dict):
    """Publish a JSON payload to the RabbitMQ exchange."""
    global _rmq_conn, _rmq_ch
    try:
        if _rmq_conn is None or _rmq_conn.is_closed:
            _rmq_conn, _rmq_ch = get_connection()
        _rmq_ch.basic_publish(
            exchange=config.RABBITMQ_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        logger.info(f"Published to RabbitMQ [{routing_key}]: {payload}")
    except Exception as e:
        logger.error(f"RabbitMQ publish error: {e}")
        _rmq_conn = None


# ---------------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------------
async def http_telemetry(request):
    """Accept POST /telemetry with JSON body → publish to RabbitMQ."""
    try:
        data = await request.json()
        data["protocol"] = "HTTP"
        data["received_at"] = time.time()
        routing_key = f"sensor.{data.get('device_id', 'unknown')}"
        publish_to_rabbitmq(routing_key, data)
        return web.json_response({"status": "ok", "protocol": "HTTP"})
    except Exception as e:
        return web.json_response({"status": "error", "detail": str(e)}, status=400)


async def http_alert(request):
    """Accept POST /alert with JSON body → publish to RabbitMQ."""
    try:
        data = await request.json()
        data["protocol"] = "HTTP"
        data["alert"] = True
        data["received_at"] = time.time()
        routing_key = f"sensor.alert.{data.get('device_id', 'unknown')}"
        publish_to_rabbitmq(routing_key, data)
        return web.json_response({"status": "ok", "protocol": "HTTP", "alert": True})
    except Exception as e:
        return web.json_response({"status": "error", "detail": str(e)}, status=400)


# ---------------------------------------------------------------------------
# CoAP resource
# ---------------------------------------------------------------------------
class TelemetryResource(resource.Resource):
    """CoAP resource at /telemetry — accepts POST with JSON payload."""

    async def render_post(self, request):
        try:
            payload = json.loads(request.payload.decode())
            payload["protocol"] = "CoAP"
            payload["received_at"] = time.time()
            routing_key = f"sensor.{payload.get('device_id', 'unknown')}"
            publish_to_rabbitmq(routing_key, payload)
            return Message(code=Code.CHANGED, payload=b"ok")
        except Exception as e:
            return Message(code=Code.BAD_REQUEST, payload=str(e).encode())


class AlertResource(resource.Resource):
    """CoAP resource at /alert — accepts POST for urgent events."""

    async def render_post(self, request):
        try:
            payload = json.loads(request.payload.decode())
            payload["protocol"] = "CoAP"
            payload["alert"] = True
            payload["received_at"] = time.time()
            routing_key = f"sensor.alert.{payload.get('device_id', 'unknown')}"
            publish_to_rabbitmq(routing_key, payload)
            return Message(code=Code.CHANGED, payload=b"ok")
        except Exception as e:
            return Message(code=Code.BAD_REQUEST, payload=str(e).encode())


# ---------------------------------------------------------------------------
# MQTT subscriber (connects to RabbitMQ MQTT plugin)
# ---------------------------------------------------------------------------
def mqtt_subscriber():
    """Subscribe to MQTT topics via RabbitMQ MQTT plugin and republish."""
    import paho.mqtt.client as mqtt

    def on_connect(client, userdata, flags, rc, properties=None):
        logger.info(f"MQTT subscriber connected (rc={rc})")
        client.subscribe(config.MQTT_TOPIC + "/#")
        client.subscribe(config.MQTT_ALERT_TOPIC + "/#")
        logger.info(f"Subscribed to {config.MQTT_TOPIC}/# and {config.MQTT_ALERT_TOPIC}/#")

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            data["protocol"] = "MQTT"
            data["received_at"] = time.time()
            # Determine routing key from topic
            topic_parts = msg.topic.split("/")
            routing_key = f"sensor.{topic_parts[-1]}" if len(topic_parts) > 1 else "sensor.unknown"
            if "alert" in msg.topic:
                routing_key = f"sensor.alert.{data.get('device_id', 'unknown')}"
                data["alert"] = True
            publish_to_rabbitmq(routing_key, data)
        except Exception as e:
            logger.error(f"MQTT message error: {e}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="gateway-mqtt-subscriber")
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    client.connect(config.MQTT_HOST, config.MQTT_PORT, 60)
    logger.info("MQTT subscriber loop started")
    client.loop_forever()


# ---------------------------------------------------------------------------
# Main — start all servers
# ---------------------------------------------------------------------------
async def main():
    # Setup RabbitMQ topology
    setup_rabbitmq()
    logger.info("RabbitMQ topology ready")

    # Start MQTT subscriber in a thread
    import threading
    mqtt_thread = threading.Thread(target=mqtt_subscriber, daemon=True)
    mqtt_thread.start()
    logger.info("MQTT subscriber thread started")

    # Start CoAP server
    coap_root = resource.Site()
    coap_root.add_resource(["telemetry"], TelemetryResource())
    coap_root.add_resource(["alert"], AlertResource())
    coap_context = await aiocoap.Context.create_server_context(coap_root, bind=(config.COAP_HOST, config.COAP_PORT))
    logger.info(f"CoAP server listening on {config.COAP_HOST}:{config.COAP_PORT}")

    # Start HTTP server
    app = web.Application()
    app.router.add_post("/telemetry", http_telemetry)
    app.router.add_post("/alert", http_alert)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.GATEWAY_HTTP_HOST, config.GATEWAY_HTTP_PORT)
    await site.start()
    logger.info(f"HTTP server listening on {config.GATEWAY_HTTP_HOST}:{config.GATEWAY_HTTP_PORT}")

    logger.info("=" * 60)
    logger.info("Gateway running — HTTP :8080 | MQTT :1883 | CoAP :5683")
    logger.info("=" * 60)

    # Keep running
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await coap_context.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
