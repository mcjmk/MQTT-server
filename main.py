# main.py

from connection.server import Server

if __name__ == "__main__":
    # Example usage:
    # To enable authentication, set authentication=True
    server = Server(host='127.0.0.1', port=1884, authentication=True)
    server.run()
