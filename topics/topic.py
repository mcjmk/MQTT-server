from messages import PublishMessage


class Topic:
    def __init__(self, topic_name: str):
        self.topic_name: str = topic_name