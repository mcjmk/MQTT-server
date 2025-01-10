import abc

# from MQTT import MQTTBroker

class MQTTCommand(abc.ABC):
    @abc.abstractmethod
    def execute(self, broker: 'MQTTBroker'):
        """"""
        pass