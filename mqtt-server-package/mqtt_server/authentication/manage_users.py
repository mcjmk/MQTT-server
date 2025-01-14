# scripts/manage_users.py

import argparse
import logging
import sys
import base64
import json
import os

from .user_auth import UserAccount, UserAuthenticator
from .device_pairing_manager import DevicePairingManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('UserManager')

# File to persist user data
USER_DATA_FILE = 'users.json'

def load_user_data(authenticator: UserAuthenticator, device_manager: DevicePairingManager):
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, 'r') as f:
            data = json.load(f)
            for username, info in data.get('users', {}).items():
                # Load hashed password
                password_hash = base64.b64decode(info['password_hash'].encode('utf-8'))
                user_account = UserAccount(username, password='')  # Temporary password
                user_account.password_hash = password_hash  # Set the hashed password directly
                authenticator.users[username] = user_account
                # Restore paired devices
                for device_id in info.get('paired_devices', []):
                    device_manager.pair_device(username, device_id)
                # Restore authorized topics
                for topic in info.get('authorized_topics', []):
                    device_manager.authorize_topic(username, topic)
        logger.info("Loaded user data from file.")
    else:
        logger.info("No existing user data found. Starting fresh.")

def save_user_data(authenticator: UserAuthenticator, device_manager: DevicePairingManager):
    data = {'users': {}}
    for username, user in authenticator.users.items():
        data['users'][username] = {
            'password_hash': base64.b64encode(user.password_hash).decode('utf-8'),
            'paired_devices': user.paired_devices,
            'authorized_topics': user.authorized_topics
        }
    with open(USER_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=4)
    logger.info("Saved user data to file.")

def main():
    parser = argparse.ArgumentParser(description='Manage MQTT Broker Users')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Register User
    register_parser = subparsers.add_parser('register', help='Register a new user')
    register_parser.add_argument('username', type=str, help='Username for the new user')
    register_parser.add_argument('password', type=str, help='Password for the new user')
    
    # Pair Device
    pair_parser = subparsers.add_parser('pair', help='Pair a device with a user')
    pair_parser.add_argument('username', type=str, help='Username of the user')
    pair_parser.add_argument('device_id', type=str, help='Device ID to pair')
    
    # Authorize Topic
    auth_parser = subparsers.add_parser('authorize', help='Authorize a topic for a user')
    auth_parser.add_argument('username', type=str, help='Username of the user')
    auth_parser.add_argument('topic', type=str, help='Topic to authorize')
    
    # List Users
    list_parser = subparsers.add_parser('list', help='List all users')
    
    # Remove User
    remove_parser = subparsers.add_parser('remove', help='Remove a user')
    remove_parser.add_argument('username', type=str, help='Username of the user to remove')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    # Initialize authenticator and device manager
    authenticator = UserAuthenticator()
    device_manager = DevicePairingManager(authenticator=authenticator)
    
    # Load existing user data
    load_user_data(authenticator, device_manager)
    
    if args.command == 'register':
        success = authenticator.register(args.username, args.password)
        if success:
            logger.info(f"User '{args.username}' registered successfully.")
            save_user_data(authenticator, device_manager)
        else:
            logger.error(f"Failed to register user '{args.username}'.")
    
    elif args.command == 'pair':
        success = device_manager.pair_device(args.username, args.device_id)
        if success:
            logger.info(f"Device '{args.device_id}' paired with user '{args.username}'.")
            save_user_data(authenticator, device_manager)
        else:
            logger.error(f"Failed to pair device '{args.device_id}' with user '{args.username}'.")
    
    elif args.command == 'authorize':
        success = device_manager.authorize_topic(args.username, args.topic)
        if success:
            logger.info(f"Topic '{args.topic}' authorized for user '{args.username}'.")
            save_user_data(authenticator, device_manager)
        else:
            logger.error(f"Failed to authorize topic '{args.topic}' for user '{args.username}'.")
    
    elif args.command == 'list':
        if not authenticator.users:
            logger.info("No users registered.")
        else:
            logger.info("Registered Users:")
            for username, user in authenticator.users.items():
                logger.info(f"- {username}")
                logger.info(f"  Paired Devices: {', '.join(user.paired_devices) if user.paired_devices else 'None'}")
                logger.info(f"  Authorized Topics: {', '.join(user.authorized_topics) if user.authorized_topics else 'None'}")
    
    elif args.command == 'remove':
        if args.username not in authenticator.users:
            logger.error(f"User '{args.username}' does not exist.")
        else:
            del authenticator.users[args.username]
            logger.info(f"User '{args.username}' removed successfully.")
            save_user_data(authenticator, device_manager)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
