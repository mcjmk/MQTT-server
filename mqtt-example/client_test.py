import asyncio
from mqtt_server import Client

async def main():
    # Create the client instance
    client = Client("user1", "user1", "password123", host='127.0.0.1', port=1884)
    
    # Connect to the server
    if not await client.connect():
        print("Failed to connect to the server.")
        return
    
    # Subscribe to a topic
    await client.subscribe("topic1", qos=1)
    
    # Publish a message to a topic
    await client.publish("topic1", "Hello, World!", qos=1)
    
    # Start listening for messages (optional)
    listen_task = asyncio.create_task(client.listen())
    
    # Run for a short while to demonstrate
    await asyncio.sleep(5)
    
    # Disconnect
    await client.disconnect()
    
    
    # Cancel the listening task if still running
    listen_task.cancel()
    try:
        await listen_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    # Run the asyncio event loop
    asyncio.run(main())
