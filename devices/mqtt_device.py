"""
MQTT device simulator — publishes telemetry and alerts via MQTT.
Demonstrates publish/subscribe communication model.
"""
import json
import time
import random
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import paho.mqtt.client as mqtt
import config


def generate_sensor_data(device_id):
    """Generate realistic smart agriculture sensor readings."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(18.0, 42.0), 2),
        "humidity": round(random.uniform(30.0, 95.0), 2),
        "soil_moisture": round(random.uniform(10.0, 80.0), 2),
    }


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[MQTT Device] Connected to broker successfully")
    else:
        print(f"[MQTT Device] Connection failed (rc={rc})")


def on_publish(client, userdata, mid, rc, properties=None):
    print(f"  → Message {mid} published (acknowledged)")


def send_telemetry(device_id, count=10, interval=2):
    """Send periodic telemetry via MQTT PUBLISH."""
    print(f"[MQTT Device {device_id}] Starting telemetry ({count} messages, {interval}s interval)")
    print(f"[MQTT Device {device_id}] Protocol model: Publish/Subscribe (fire-and-forget with QoS 0)")
    print("-" * 60)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"mqtt-device-{device_id}")
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.username_pw_set(config.RABBITMQ_USER, config.RABBITMQ_PASS)

    client.connect(config.MQTT_HOST, config.MQTT_PORT, 60)
    client.loop_start()

    # Wait for connection
    time.sleep(1)

    latencies = []
    for i in range(count):
        data = generate_sensor_data(device_id)
        data["seq"] = i + 1
        topic = f"{config.MQTT_TOPIC}/{device_id}"

        start = time.time()
        result = client.publish(
            topic,
            payload=json.dumps(data),
            qos=0,  # Fire and forget — no ACK required from broker for minimal latency
        )
        elapsed = time.time() - start

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            latencies.append(elapsed)
            print(f"  [{i+1}/{count}] PUBLISH {topic} ({elapsed*1000:.1f}ms) "
                  f"T={data['temperature']}°C  H={data['humidity']}%  SM={data['soil_moisture']}%")
        else:
            print(f"  [{i+1}/{count}] PUBLISH FAILED (rc={result.rc})")

        if i < count - 1:
            time.sleep(interval)

    if latencies:
        print(f"\n  Average publish time: {sum(latencies)/len(latencies)*1000:.1f}ms")
        print(f"  Min/Max publish time: {min(latencies)*1000:.1f}ms / {max(latencies)*1000:.1f}ms")
    print()

    client.loop_stop()
    client.disconnect()


def send_alert(device_id):
    """Send an urgent alert via MQTT."""
    print(f"[MQTT Device {device_id}] Sending ALERT: soil moisture too low!")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"mqtt-alert-{device_id}")
    client.on_connect = on_connect
    client.username_pw_set(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    client.connect(config.MQTT_HOST, config.MQTT_PORT, 60)
    client.loop_start()
    time.sleep(1)

    data = {
        "device_id": device_id,
        "alert_type": "soil_moisture_low",
        "soil_moisture": 5.2,
        "message": "CRITICAL: Soil moisture below minimum threshold!",
    }
    topic = f"{config.MQTT_ALERT_TOPIC}/{device_id}"

    start = time.time()
    result = client.publish(
        topic,
        payload=json.dumps(data),
        qos=1,  # At least once delivery for alerts
    )
    elapsed = time.time() - start

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"  PUBLISH {topic} (QoS 1, {elapsed*1000:.1f}ms)")
    else:
        print(f"  PUBLISH FAILED")

    time.sleep(1)
    client.loop_stop()
    client.disconnect()
    print()


if __name__ == "__main__":
    device_id = "mqtt-sensor-01"
    send_telemetry(device_id, count=10, interval=2)
    send_alert(device_id)
    print("[MQTT Device] Done. Pub/Sub model confirmed — device fires and does not wait for backend processing.")
