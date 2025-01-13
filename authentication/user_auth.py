import logging
import hashlib
from dataclasses import dataclass, field
from typing import Dict, List

import bcrypt

from utils.singleton import Singleton


@dataclass
class Users:
    Users_User: str
    Users_Pass: str

@dataclass
class UserAccount:
    username: str
    password_hash: bytes  # Store hash as bytes
    paired_devices: List[str] = field(default_factory=list)
    authorized_topics: List[str] = field(default_factory=list)

    def __init__(self, username: str, password: str):
        self.username = username
        self.password_hash = self._hash_password(password)
        self.paired_devices = []
        self.authorized_topics = []

    def _hash_password(self, password: str) -> bytes:
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    def verify_password(self, password: str) -> bool:
        try:
            return bcrypt.checkpw(password.encode('utf-8'), self.password_hash)
        except ValueError as e:
            self.logger = logging.getLogger('UserAccount')
            self.logger.error(f"Password verification failed: {e}")
            return False

class UserAuthenticator(metaclass=Singleton):
    def __init__(self, save_callback=None):
        self.users: Dict[str, UserAccount] = {}
        self.logger = logging.getLogger('UserAuthenticator')
        self.save_callback = save_callback  # Callback to save data

    def register(self, username: str, password: str) -> bool:
        if username in self.users:
            self.logger.warning(f"User {username} already exists")
            return False
        self.users[username] = UserAccount(username, password)
        self.logger.info(f"User {username} registered successfully")
        if self.save_callback:
            self.save_callback()
        return True

    def login(self, username: str, password: str) -> bool:
        if username not in self.users:
            self.logger.warning(f"User {username} does not exist")
            return False
        user_account = self.users[username]
        if user_account.verify_password(password):
            self.logger.info(f"User {username} logged in successfully")
            return True
        self.logger.warning(f"Invalid password for user {username}")
        return False

    # Optionally, implement methods to remove or modify users with save_callback
