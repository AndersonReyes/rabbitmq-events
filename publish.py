from events.rabbitmqclient import RabbitMQEventsClient
import sys
import json

with RabbitMQEventsClient("publish") as client:
    message = " ".join(sys.argv[1:]) or "Hello World!"

    client.send_event("test-event", message)
