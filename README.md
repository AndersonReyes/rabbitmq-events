# rabbitmq-events

Simple experiment of writing a basic rabbitmq event library.
It uses json for body

Example consumer for `test-event` events:
```python

client = RabbitMQEventsClient("ExampleConsumer")

@client.task("test-event")
def handler(body):
    print(body)

@client.task("test-event")
def handler2(body):
    print(body)

client.start()
```


To send events:
```python
client = RabbitMQEventsClient("Publisher")
client.send_event("test-event", body={"hello": "world"})
```
