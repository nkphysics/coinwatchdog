from datetime import datetime
from datetime import timedelta
import pathlib as pl
import argparse
import logging
import sys
import os
from importlib.metadata import version
import sqlite3
import asyncio
import json
import websockets


COINWATCHDOG = os.path.basename(sys.argv[0])
VERSION = version('COINWATCHDOG')
logger = logging.getLogger(COINWATCHDOG)

URI = 'wss://ws-feed.exchange.coinbase.com'
SIGNATURE_PATH = '/users/self/verify'

channel = 'ticker_batch'


class DBMan:
    def __init__(self, db_name):
        db_name = pl.Path(db_name).resolve()
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.pair_db_ids = self._get_product_ids()

    def close_connection(self):
        """
        Close the database connection.
        """
        self.conn.close()

    def _get_product_ids(self) -> dict:
        """
        Gets all database int id pairs for each product_id

        Returns:
        dict, dictionary with product_id str: product_id int
        """
        pair_db_ids = {}
        pid_pairs = self.cursor.execute("""SELECT * FROM products;""")
        for pair in pid_pairs:
            pair_db_ids[pair[1]] = pair[0]
        return pair_db_ids

    async def _insert_pricing_data(self, data: dict) -> int:
        """
        Inserts transformed pricing data into the pricing_data table

        Parameters:
        data: dict, all the transformed pricing data

        Returns:
        int, id of the added row
        """
        logger.info(f"Data loading to database for {data['product_id']}"
                    f" @ {data['last_timestamp']}")
        data["product_id"] = self.pair_db_ids[data["product_id"]]
        self.cursor.execute("""INSERT INTO pricing_data (
                                product_id,
                                last_timestamp,
                                open_price,
                                close_price,
                                min_price,
                                max_price,
                                open_volume_24h,
                                close_volume_24h,
                                min_volume_24h,
                                max_volume_24h,
                                open_volume_30d,
                                close_volume_30d,
                                min_volume_30d,
                                max_volume_30d,
                                open_best_bid,
                                close_best_bid,
                                min_best_bid,
                                max_best_bid,
                                open_best_bid_size,
                                close_best_bid_size,
                                min_best_bid_size,
                                max_best_bid_size,
                                open_best_ask,
                                close_best_ask,
                                min_best_ask,
                                max_best_ask,
                                open_best_ask_size,
                                close_best_ask_size,
                                min_best_ask_size,
                                max_best_ask_size
                            )
                               VALUES (
                                :product_id,
                                :last_timestamp,
                                :open_price,
                                :close_price,
                                :min_price,
                                :max_price,
                                :open_volume_24h,
                                :close_volume_24h,
                                :min_volume_24h,
                                :max_volume_24h,
                                :open_volume_30d,
                                :close_volume_30d,
                                :min_volume_30d,
                                :max_volume_30d,
                                :open_best_bid,
                                :close_best_bid,
                                :min_best_bid,
                                :max_best_bid,
                                :open_best_bid_size,
                                :close_best_bid_size,
                                :min_best_bid_size,
                                :max_best_bid_size,
                                :open_best_ask,
                                :close_best_ask,
                                :min_best_ask,
                                :max_best_ask,
                                :open_best_ask_size,
                                :close_best_ask_size,
                                :min_best_ask_size,
                                :max_best_ask_size
                               )""",
                            data)
        self.conn.commit()
        return self.cursor.lastrowid


async def time_diff(time1: str, time2: str) -> int:
    """
    Computes the difference between two different iso format timestamps

    Parameters:
    time1: str, first time in iso zulu format

    time2: str, subsequent (future) time in iso zulu format

    Returns:
    int, minutes between times
    """
    dt1 = datetime.fromisoformat(time1[:-1])
    dt2 = datetime.fromisoformat(time2[:-1])
    return (dt2 - dt1).seconds // 60


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
    logger.debug(f"Transformed 60s data: \n{mm_dict}")
    return mm_dict


async def websocket_listener(db_name):

    db = DBMan(db_name)
    product_ids = list(db.pair_db_ids.keys())
    subscribe_message = json.dumps({
        'type': 'subscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    reconnect_attempt = 0

    while True:
        info = {}
        try:
            async with websockets.connect(
                URI, ping_interval=20, ping_timeout=20
            ) as socket:
                await socket.send(subscribe_message)
                logger.info(f"Connected to {URI}")
                logger.info(f"Subscribed to {channel} for {product_ids}")
                reconnect_attempt = 0
                while True:
                    response = await asyncio.wait_for(
                        socket.recv(), timeout=60
                    )
                    json_response = json.loads(response)
                    if json_response["type"] == "ticker":
                        pair = json_response["product_id"]
                        if pair in info:
                            ipair = info[pair]
                            ipair.append(json_response)
                            if len(ipair) > 1:
                                tdiff = await time_diff(ipair[0]["time"],
                                                        ipair[-1]["time"])
                                if tdiff >= 1:
                                    block = await make_min_metrics(
                                        ipair.copy()
                                    )
                                    await db._insert_pricing_data(block)
                                    info[pair] = []
                        else:
                            info[pair] = [json_response]
                        logger.debug(f"Data Recieved for pair: {pair}")
                        logger.debug(f"Size of {pair} buffer: "
                                     f"{len(info[pair])}")
                        # logger(json_response)

        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK) as e:
            reconnect_attempt += 1
            delay = min(2 ** reconnect_attempt, 60)
            logger.error(f"Connection closed ({e}), "
                         f"reconnecting in {delay}s "
                         f"(attempt {reconnect_attempt})..")
            await asyncio.sleep(delay)

        except asyncio.TimeoutError:
            reconnect_attempt += 1
            delay = min(2 ** reconnect_attempt, 60)
            logger.error(f"No message received within 60s, "
                         f"reconnecting in {delay}s "
                         f"(attempt {reconnect_attempt})..")
            await asyncio.sleep(delay)

        except (OSError, websockets.exceptions.InvalidHandshake) as e:
            reconnect_attempt += 1
            delay = min(2 ** reconnect_attempt, 60)
            logger.error(f"Connection error ({e}), "
                         f"reconnecting in {delay}s "
                         f"(attempt {reconnect_attempt})..")
            await asyncio.sleep(delay)

        except Exception as e:
            reconnect_attempt += 1
            delay = min(2 ** reconnect_attempt, 60)
            logger.error(f"Unexpected error ({type(e).__name__}: {e}), "
                         f"reconnecting in {delay}s "
                         f"(attempt {reconnect_attempt})..")
            await asyncio.sleep(delay)


def cli():
    p = argparse.ArgumentParser(
        description=("ETL pipeline tool for coinbase market data")
    )
    p.add_argument(
        "--database",
        "-db",
        type=str,
        default=None,
        dest="database",
        help="Path to sqlite database to connect to"
    )
    p.add_argument(
        "--log",
        "-l",
        type=str,
        default=None,
        dest="logfile",
        help="Path to output log file"
    )
    p.add_argument(
        "--debug",
        action="store_true",
        default=False,
        dest="debug",
        help="Engage debug mode for logging"
    )
    p.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {VERSION}"
    )
    argp = p.parse_args()
    return argp


def main(opts: argparse.Namespace):
    level = logging.INFO
    if opts.debug is True:
        level = logging.DEBUG

    if opts.logfile is not None:
        filelog = logging.FileHandler(pl.Path(opts.logfile).resolve())
        fmat = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
        filelog.setFormatter(fmat)
        logger.addHandler(filelog)

    logging.basicConfig(stream=sys.stdout,
                        level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger.info("COINWATCHDOG STARTED")
    try:
        asyncio.run(websocket_listener(opts.database))
    except KeyboardInterrupt:
        logger.info("Exiting WebSocket..")


def entry():
    opts = cli()
    main(opts)
