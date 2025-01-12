import logging
from typing import Dict
from authentication.user_auth import UserAccount  

class DevicePairingManager:
    def __init__(self):
        self.user_accounts: Dict[str, UserAccount] = {}
        self.logger = logging.getLogger('DevicePairingManager')

    def register_user(self, username: str, password: str) -> bool:
        if username in self.user_accounts:
            self.logger.warning(f"User {username} already exists")
            return False
        self.user_accounts[username] = UserAccount(username, password)
        self.logger.info(f"User {username} registered successfully")
        return True

    def pair_device(self, username: str, device_id: str) -> bool:
        if username not in self.user_accounts:
            self.logger.warning(f"User {username} does not exist")
            return False
        user_account = self.user_accounts[username]
        if device_id not in user_account.paired_devices:
            user_account.paired_devices.append(device_id)
            self.logger.info(f"Device {device_id} paired with user {username}")
            return True
        self.logger.warning(f"Device {device_id} is already paired with user {username}")
        return False

    def authorize_topic(self, username: str, topic: str) -> bool:
        if username not in self.user_accounts:
            self.logger.warning(f"User {username} does not exist")
            return False
        user_account = self.user_accounts[username]
        if topic not in user_account.authorized_topics:
            user_account.authorized_topics.append(topic)
            self.logger.info(f"Topic {topic} authorized for user {username}")
            return True
        return False

    def is_device_authorized(self, username: str, device_id: str) -> bool:
        if username not in self.user_accounts:
            return False
        return device_id in self.user_accounts[username].paired_devices

    def is_topic_authorized(self, username: str, topic: str) -> bool:
        if username not in self.user_accounts:
            return False
        return topic in self.user_accounts[username].authorized_topics