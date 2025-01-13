# authentication/device_pairing_manager.py

import logging
from typing import Dict
from authentication.user_auth import UserAccount, UserAuthenticator 

# authentication/device_pairing_manager.py

class DevicePairingManager:
    def __init__(self, authenticator: UserAuthenticator, save_callback=None):
        """
        Initialize with a reference to the UserAuthenticator and a save callback.
        """
        self.authenticator = authenticator  # Reference to UserAuthenticator
        self.logger = logging.getLogger('DevicePairingManager')
        self.save_callback = save_callback  # Callback to save data

    def pair_device(self, username: str, device_id: str) -> bool:
        if username not in self.authenticator.users:
            self.logger.warning(f"User {username} does not exist")
            return False
        user_account = self.authenticator.users[username]
        if device_id not in user_account.paired_devices:
            user_account.paired_devices.append(device_id)
            self.logger.info(f"Device '{device_id}' paired with user '{username}'")
            if self.save_callback:
                self.save_callback()
            return True
        self.logger.warning(f"Device '{device_id}' is already paired with user '{username}'")
        return False

    def authorize_topic(self, username: str, topic: str) -> bool:
        if username not in self.authenticator.users:
            self.logger.warning(f"User {username} does not exist")
            return False
        user_account = self.authenticator.users[username]
        if topic not in user_account.authorized_topics:
            user_account.authorized_topics.append(topic)
            self.logger.info(f"Topic '{topic}' authorized for user '{username}'")
            if self.save_callback:
                self.save_callback()
            return True
        self.logger.warning(f"Topic '{topic}' is already authorized for user '{username}'")
        return False

    def is_device_authorized(self, username: str, device_id: str) -> bool:
        if username not in self.authenticator.users:
            return False
        return device_id in self.authenticator.users[username].paired_devices

    def is_topic_authorized(self, username: str, topic: str) -> bool:
        print("hello", username, topic, self.authenticator.users[username].authorized_topics)
        if username not in self.authenticator.users:
            return False
        return topic in self.authenticator.users[username].authorized_topics
