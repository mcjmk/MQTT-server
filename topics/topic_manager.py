from topics.topic import Topic
from utils.singleton import Singleton


class TopicManager(metaclass=Singleton):
    def __init__(self):
        self.topics: dict[str, Topic] = dict()
