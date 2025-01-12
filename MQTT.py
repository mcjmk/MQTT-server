import abc
import uuid
import threading
import queue
import logging
from typing import Dict, List, Callable

from utils.mqtt_command import MQTTCommand
from utils.observer import Observer
from utils.singleton import Singleton


"""Przeniesone do utils"""
# class Observer(abc.ABC):
# class Singleton(type):
# class MQTTCommand(abc.ABC):



class PublishCommand(MQTTCommand):
    def __init__(self, topic: str, message: str, sender: 'MQTTClient'):
        self.topic = topic
        self.message = message
        self.sender = sender

    def execute(self, broker: 'MQTTBroker'):
        broker.broadcast_message(self.topic, self.message, self.sender)


class SubscribeCommand(MQTTCommand):
    def __init__(self, client: 'MQTTClient', topic: str):
        self.client = client
        self.topic = topic

    def execute(self, broker: 'MQTTBroker'):
        broker.add_subscriber(self.topic, self.client)


class UnsubscribeCommand(MQTTCommand):
    def __init__(self, client: 'MQTTClient', topic: str):
        self.client = client
        self.topic = topic

    def execute(self, broker: 'MQTTBroker'):
        broker.remove_subscriber(self.topic, self.client)


class MQTTClient(Observer):
    def __init__(self, name: str = None):
        self.client_id = name or str(uuid.uuid4())
        self.message_handlers: Dict[str, Callable] = {}
        self.logger = logging.getLogger(f'MQTTClient-{self.client_id}')

    def subscribe(self, broker: 'MQTTBroker', topic: str, handler: Callable = None):
        """"""
        subscribe_cmd = SubscribeCommand(self, topic)
        broker.process_command(subscribe_cmd)

        if handler:
            self.message_handlers[topic] = handler

    def unsubscribe(self, broker: 'MQTTBroker', topic: str):
        """"""
        unsubscribe_cmd = UnsubscribeCommand(self, topic)
        broker.process_command(unsubscribe_cmd)

        if topic in self.message_handlers:
            del self.message_handlers[topic]

    def publish(self, broker: 'MQTTBroker', topic: str, message: str):
        """"""
        publish_cmd = PublishCommand(topic, message, self)
        broker.process_command(publish_cmd)

    def update(self, topic: str, message: str):
        """"""
        self.logger.info(f"Client {self.client_id} received message on {topic}: {message}")

        handler = self.message_handlers.get(topic)
        if handler:
            handler(topic, message)


class MQTTBroker(metaclass=Singleton):
    def __init__(self):

        if not hasattr(self, 'initialized'):
            self.topic_subscribers: Dict[str, List[Observer]] = {}
            self.logger = logging.getLogger('MQTTBroker')

            self.command_queue = queue.Queue()
            self.running = False
            self.command_thread = None

            self.initialized = True

    def get_instance_id(self):

        return id(self)

    def start(self):
        """"""
        self.running = True
        self.command_thread = threading.Thread(target=self._process_commands)
        self.command_thread.start()
        self.logger.info("MQTT Broker started")

    def stop(self):
        """"""
        self.running = False
        if self.command_thread:
            self.command_thread.join()
        self.logger.info("MQTT Broker stopped")

    def _process_commands(self):
        """"""
        while self.running:
            try:
                command = self.command_queue.get(timeout=1)
                command.execute(self)
            except queue.Empty:
                continue

    def process_command(self, command: MQTTCommand):
        """"""
        self.command_queue.put(command)

    def add_subscriber(self, topic: str, subscriber: Observer):
        """"""
        if topic not in self.topic_subscribers:
            self.topic_subscribers[topic] = []

        if subscriber not in self.topic_subscribers[topic]:
            self.topic_subscribers[topic].append(subscriber)
            self.logger.info(f"Subscriber added to topic: {topic}")

    def remove_subscriber(self, topic: str, subscriber: Observer):
        """"""
        if topic in self.topic_subscribers and subscriber in self.topic_subscribers[topic]:
            self.topic_subscribers[topic].remove(subscriber)
            self.logger.info(f"Subscriber removed from topic: {topic}")

    def broadcast_message(self, topic: str, message: str, sender: Observer = None):
        """"""
        if topic in self.topic_subscribers:
            for subscriber in self.topic_subscribers[topic]:
                if subscriber != sender:
                    subscriber.update(topic, message)


def main():
    logging.basicConfig(level=logging.INFO)

    broker1 = MQTTBroker()
    broker2 = MQTTBroker()

    print(f"Broker 1 ID: {broker1.get_instance_id()}")
    print(f"Broker 2 ID: {broker2.get_instance_id()}")

    broker1.start()

    sensor_client = MQTTClient("sensor")
    dashboard_client = MQTTClient("dashboard")


    dashboard_client.message_handlers["temperature"] = temperature_handler
    dashboard_client.message_handlers["humidity"] = humidity_handler

    dashboard_client.subscribe(broker1, "temperature")
    dashboard_client.subscribe(broker1, "humidity")

    sensor_client.publish(broker1, "temperature", "25°C")
    sensor_client.publish(broker1, "humidity", "45%")


    import time
    time.sleep(0.5)

    # broker1.stop()

    # print("IF broker1 same as broker2:", broker1 is broker2)


if __name__ == "__main__":
    def temperature_handler(topic: str, message: str):
        print(f"Temperature Handler: {topic} - {message}")


    def humidity_handler(topic: str, message: str):
        print(f"Humidity Handler: {topic} - {message}")


    main()