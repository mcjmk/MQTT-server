import logging
import hashlib
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class Users:
    Users_User: str
    Users_Pass: str

class UserAccount:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password_hash = self._hash_password(password)
        self.paired_devices: List[str] = []
        self.authorized_topics: List[str] = []

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def verify_password(self, password: str) -> bool:
        return self.password_hash == self._hash_password(password)

class UserAuthenticator:
    def __init__(self):
        self.users: Dict[str, UserAccount] = {}
        self.logger = logging.getLogger('UserAuthenticator')

    def register(self, username: str, password: str) -> bool:
        if username in self.users:
            self.logger.warning(f"User {username} already exists")
            return False
        self.users[username] = UserAccount(username, password)
        self.logger.info(f"User {username} registered successfully")
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