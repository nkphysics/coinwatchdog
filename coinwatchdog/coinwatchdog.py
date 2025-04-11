
import asyncio
import json
import websockets


URI = 'wss://ws-feed.exchange.coinbase.com'
SIGNATURE_PATH = '/users/self/verify'

channel = 'ticker_batch'
product_ids = ['BTC-USD', 'ADA-USD']


async def websocket_listener():
    subscribe_message = json.dumps({
        'type': 'subscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    unsubscribe_message = json.dumps({
        'type': 'unsubscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    while True:
        try:
            async with websockets.connect(URI, ping_interval=None) as socket:
                await socket.send(subscribe_message)
                while True:
                    response = await socket.recv()
                    json_response = json.loads(response)
                    print(json_response)

        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)


if __name__ == '__main__':
    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
