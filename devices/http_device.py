"""
HTTP device simulator — sends telemetry and alerts via HTTP POST.
Demonstrates request/response communication model.
"""
import json
import time
import random
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import config

GATEWAY_URL = f"http://{config.GATEWAY_HTTP_HOST}:{config.GATEWAY_HTTP_PORT}"


def generate_sensor_data(device_id):
    """Generate realistic smart agriculture sensor readings."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(18.0, 42.0), 2),
        "humidity": round(random.uniform(30.0, 95.0), 2),
        "soil_moisture": round(random.uniform(10.0, 80.0), 2),
    }


def send_telemetry(device_id, count=10, interval=2):
    """Send periodic telemetry via HTTP POST."""
    print(f"[HTTP Device {device_id}] Starting telemetry ({count} messages, {interval}s interval)")
    print(f"[HTTP Device {device_id}] Protocol model: Request/Response")
    print("-" * 60)

    latencies = []
    for i in range(count):
        data = generate_sensor_data(device_id)
        data["seq"] = i + 1

        start = time.time()
        try:
            resp = requests.post(
                f"{GATEWAY_URL}/telemetry",
                json=data,
                timeout=5,
            )
            elapsed = time.time() - start
            latencies.append(elapsed)
            status = resp.status_code
            print(f"  [{i+1}/{count}] POST /telemetry → {status} ({elapsed*1000:.1f}ms) "
                  f"T={data['temperature']}°C  H={data['humidity']}%  SM={data['soil_moisture']}%")
        except Exception as e:
            print(f"  [{i+1}/{count}] ERROR: {e}")

        if i < count - 1:
            time.sleep(interval)

    if latencies:
        print(f"\n  Average latency: {sum(latencies)/len(latencies)*1000:.1f}ms")
        print(f"  Min/Max latency: {min(latencies)*1000:.1f}ms / {max(latencies)*1000:.1f}ms")
    print()


def send_alert(device_id):
    """Send an urgent alert via HTTP POST."""
    print(f"[HTTP Device {device_id}] Sending ALERT: temperature too high!")
    data = {
        "device_id": device_id,
        "alert_type": "temperature_high",
        "temperature": 45.7,
        "message": "CRITICAL: Greenhouse temperature exceeds threshold!",
    }
    try:
        start = time.time()
        resp = requests.post(f"{GATEWAY_URL}/alert", json=data, timeout=5)
        elapsed = time.time() - start
        print(f"  POST /alert → {resp.status_code} ({elapsed*1000:.1f}ms)")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()


if __name__ == "__main__":
    device_id = "http-sensor-01"
    send_telemetry(device_id, count=10, interval=2)
    send_alert(device_id)
    print("[HTTP Device] Done. Request/response model confirmed — each send waits for server ACK.")
