
import asyncio
import json
import websockets


URI = 'wss://ws-feed.exchange.coinbase.com'
SIGNATURE_PATH = '/users/self/verify'

channel = 'ticker_batch'
product_ids = ['BTC-USD', 'ADA-USD', "POL-USD"]


async def make_min_metrics(data: list[dict]) -> dict:
    """
    Makes all relevant metrics from the past 1 minute of data

    Parameters:
    data: list[dict], all responses ideally every 5 secs from the past min
                      of responses for a specific pair

    Returns:
    dict, single set of metrics summarizing the past 60s of data for a pair
    """
    mm_dict = {"product_id": data[0]["product_id"],
               "last_timestamp": data[-1]["time"]}

    def make_mmoc(metric: str):
        """
        Makes the min, max, open, and close metrics
        """
        values = [i[metric] for i in data]
        mm_dict[f"open_{metric}"] = values[0]
        mm_dict[f"close_{metric}"] = values[-1]
        mm_dict[f"min_{metric}"] = min(values)
        mm_dict[f"max_{metric}"] = max(values)

    metrics = ["price", "volume_24h", "volume_30d", "best_bid",
               "best_bid_size", "best_ask", "best_ask_size"]
    for metric in metrics:
        make_mmoc(metric)
    print(f"Transformed 60s data: \n{mm_dict}")


async def websocket_listener():
    subscribe_message = json.dumps({
        'type': 'subscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    unsubscribe_message = json.dumps({
        'type': 'unsubscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    info = {}

    while True:
        try:
            async with websockets.connect(URI, ping_interval=None) as socket:
                await socket.send(subscribe_message)
                while True:
                    response = await socket.recv()
                    json_response = json.loads(response)
                    if json_response["type"] == "ticker":
                        pair = json_response["product_id"]
                        if pair in info:
                            info[pair].append(json_response)
                            if len(info[pair]) == 12:
                                await make_min_metrics(info[pair].copy())
                                info[pair] = []
                        else:
                            info[pair] = [json_response]
                        print(f"Data Recieved for pair: {pair}")
                        print(f"Size of {pair} buffer: {len(info[pair])}")
                        # print(json_response)

        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)


if __name__ == '__main__':
    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
