"""
CoAP device simulator — sends telemetry and alerts via CoAP.
Demonstrates constrained RESTful communication over UDP.
"""
import json
import time
import random
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiocoap import Message, Code, Context
import config

COAP_SERVER = f"coap://{config.GATEWAY_HTTP_HOST}:{config.COAP_PORT}"


def generate_sensor_data(device_id):
    """Generate realistic smart agriculture sensor readings."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(18.0, 42.0), 2),
        "humidity": round(random.uniform(30.0, 95.0), 2),
        "soil_moisture": round(random.uniform(10.0, 80.0), 2),
    }


async def send_telemetry(device_id, count=10, interval=2):
    """Send periodic telemetry via CoAP POST."""
    print(f"[CoAP Device {device_id}] Starting telemetry ({count} messages, {interval}s interval)")
    print(f"[CoAP Device {device_id}] Protocol model: Constrained REST (request/response over UDP)")
    print("-" * 60)

    context = await Context.create_client_context()
    latencies = []

    for i in range(count):
        data = generate_sensor_data(device_id)
        data["seq"] = i + 1

        payload = json.dumps(data).encode()
        request = Message(
            code=Code.POST,
            payload=payload,
            uri=f"{COAP_SERVER}/telemetry",
        )

        start = time.time()
        try:
            response = await context.request(request).response
            elapsed = time.time() - start
            latencies.append(elapsed)
            print(f"  [{i+1}/{count}] POST /telemetry → {response.code} ({elapsed*1000:.1f}ms) "
                  f"T={data['temperature']}°C  H={data['humidity']}%  SM={data['soil_moisture']}%")
        except Exception as e:
            print(f"  [{i+1}/{count}] ERROR: {e}")

        if i < count - 1:
            await asyncio.sleep(interval)

    if latencies:
        print(f"\n  Average latency: {sum(latencies)/len(latencies)*1000:.1f}ms")
        print(f"  Min/Max latency: {min(latencies)*1000:.1f}ms / {max(latencies)*1000:.1f}ms")
    print()


async def send_alert(device_id):
    """Send an urgent alert via CoAP POST (NON-confirmable for speed)."""
    print(f"[CoAP Device {device_id}] Sending ALERT: humidity too high!")

    context = await Context.create_client_context()
    data = {
        "device_id": device_id,
        "alert_type": "humidity_high",
        "humidity": 97.5,
        "message": "CRITICAL: Humidity exceeds safe threshold — fungal risk!",
    }
    payload = json.dumps(data).encode()
    request = Message(
        code=Code.POST,
        payload=payload,
        uri=f"{COAP_SERVER}/alert",
    )

    start = time.time()
    try:
        response = await context.request(request).response
        elapsed = time.time() - start
        print(f"  POST /alert → {response.code} ({elapsed*1000:.1f}ms)")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()


async def main():
    device_id = "coap-sensor-01"
    await send_telemetry(device_id, count=5, interval=1)
    await send_alert(device_id)
    print("[CoAP Device] Done. Constrained REST confirmed — lightweight UDP-based communication.")


if __name__ == "__main__":
    asyncio.run(main())
