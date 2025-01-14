# connection/broker.py

from pathlib import Path
import asyncio
import logging
import json
import base64
from collections import defaultdict
from typing import Set, Dict, List
from dataclasses import dataclass, field

from ..authentication.user_auth import UserAuthenticator
from ..authentication.device_pairing_manager import DevicePairingManager
from ..authentication.user_auth import UserAccount
from ..messages.publish import PublishMessage
from ..utils.singleton import Singleton

@dataclass
class SessionData:
    subscriptions: Set[str] = field(default_factory=set)
    queued_messages: List[PublishMessage] = field(default_factory=list)
    queued_message_ids: Set[str] = field(default_factory=set)

class Broker(metaclass=Singleton):
    def __init__(self, authentication: bool = False):
        # Global subscription registry: topic -> set of StreamWriter objects
        self.subscriptions: defaultdict[str, Set[asyncio.StreamWriter]] = defaultdict(set)
        # Track each client's subscribed topics: StreamWriter -> set of topics
        self.client_subscriptions: defaultdict[asyncio.StreamWriter, Set[str]] = defaultdict(set)
        # Session registry: client_id -> SessionData
        self.sessions: Dict[str, SessionData] = {}
        # Connected clients: client_id -> StreamWriter
        self.connected_clients: Dict[str, asyncio.StreamWriter] = {}
        # Reverse mapping: StreamWriter -> client_id
        self.writer_to_client_id: Dict[asyncio.StreamWriter, str] = {}
        # Lock to manage concurrent access
        self.lock = asyncio.Lock()
        
        # Initialize logging
        self.logger = logging.getLogger('Broker')
        
        # Authentication
        self.authentication_enabled = authentication
        if self.authentication_enabled:
            self.authenticator = UserAuthenticator(save_callback=self.save_user_data)
            self.device_manager = DevicePairingManager(authenticator=self.authenticator, save_callback=self.save_user_data)
            self.load_user_data()
        else:
            self.authenticator = None
            self.device_manager = None

    def register_user(self, username: str, password: str) -> bool:
        """
        Registers a new user with the given username and password.
        """
        success = self.authenticator.register(username, password)
        return success

    def authorize_topic(self, username: str, topic: str) -> bool:
        """
        Authorizes a user to access a specific topic.
        """
        success = self.device_manager.authorize_topic(username, topic)
        return success
    
    def get_user_data_path(self) -> Path:
        """
        Returns the absolute Path object to the users.json file.
        """
        current_dir = Path(__file__).resolve().parent  # Directory of broker.py
        user_data_path = current_dir.parent / 'authentication' / 'users.json'
        return user_data_path

    def load_user_data(self):
        """
        Loads user data from users.json into the UserAuthenticator.
        """
        user_data_path = self.get_user_data_path()
        if user_data_path.exists():
            with user_data_path.open('r') as f:
                data = json.load(f)
                for username, info in data.get('users', {}).items():
                    # Correct key name: 'password_hash' instead of 'password'
                    password_hash = base64.b64decode(info['password_hash'].encode('utf-8'))
                    user_account = UserAccount(username, password='')  # Temporary password
                    user_account.password_hash = password_hash  # Set the hashed password directly
                    self.authenticator.users[username] = user_account
                    # Restore paired devices
                    for device_id in info.get('paired_devices', []):
                        self.device_manager.pair_device(username, device_id)
                    # Restore authorized topics
                    for topic in info.get('authorized_topics', []):
                        self.device_manager.authorize_topic(username, topic)
            self.logger.info(f"Loaded user data from {user_data_path}.")
        else:
            self.logger.info(f"No existing user data found at {user_data_path}. Starting fresh.")

    def save_user_data(self):
        """
        Saves current user data to users.json.
        """
        user_data_path = self.get_user_data_path()
        data = {'users': {}}
        for username, user in self.authenticator.users.items():
            data['users'][username] = {
                'password_hash': base64.b64encode(user.password_hash).decode('utf-8'),
                'paired_devices': user.paired_devices,
                'authorized_topics': user.authorized_topics
            }
        # Ensure the authentication directory exists
        user_data_path.parent.mkdir(parents=True, exist_ok=True)
        with user_data_path.open('w') as f:
            json.dump(data, f, indent=4)
        self.logger.info(f"Saved user data to {user_data_path}.")
