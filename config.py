"""
Shared configuration for the IoT Lab: MQTT / HTTP / CoAP + RabbitMQ.
"""

# RabbitMQ settings
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
RABBITMQ_EXCHANGE = "iot_telemetry"
RABBITMQ_EXCHANGE_TYPE = "topic"
RABBITMQ_QUEUE = "sensor_data"
RABBITMQ_ROUTING_KEY = "sensor.#"

# MQTT broker (RabbitMQ MQTT plugin)
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensor/telemetry"
MQTT_ALERT_TOPIC = "sensor/alert"

# Gateway HTTP server
GATEWAY_HTTP_HOST = "0.0.0.0"
GATEWAY_HTTP_PORT = 8090

# CoAP gateway
COAP_HOST = "0.0.0.0"
COAP_PORT = 5683

# Sensor data schema
SENSOR_FIELDS = ["temperature", "humidity", "soil_moisture", "device_id"]
