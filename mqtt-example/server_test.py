# main.py

from mqtt_server import Server

if __name__ == "__main__":
    # Example usage:
    # To enable authentication, set authentication=True
    
    server = Server(host='127.0.0.1', port=1884, authentication=True)
    server.broker.register_user('user1', 'password123')
    server.broker.register_user('user2', 'password1234')
    server.broker.authorize_topic('user1', 'topic1')
    server.broker.authorize_topic('user2', 'topic1')
    server.broker.authorize_topic('user2', 'topic2')
    server.run()
