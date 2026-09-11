from datetime import datetime
from datetime import timedelta
import pathlib as pl
import argparse
import logging
import sys
import os
from importlib.metadata import version
import asyncio
import json
import websockets

from coinwatchdog.db import get_backend
from coinwatchdog.db.timescale import TimescaleBackend


COINWATCHDOG = os.path.basename(sys.argv[0])
VERSION = version('COINWATCHDOG')
logger = logging.getLogger(COINWATCHDOG)

URI = 'wss://ws-feed.exchange.coinbase.com'
SIGNATURE_PATH = '/users/self/verify'

channel = 'ticker_batch'


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


async def websocket_listener(db):

    product_ids = list(db.pair_db_ids.keys())
    subscribe_message = json.dumps({
        'type': 'subscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}],
    })

    reconnect_attempt = 0
    is_timescaledb = isinstance(db, TimescaleBackend)

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
                        if is_timescaledb:
                            logger.debug(
                                "Data Recieved for pair: "
                                f"{json_response["product_id"]}"
                                )
                            await db.insert_ticker_row(json_response)
                        else:
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
                                        logger.info(
                                            f"Data loading to database for"
                                            f" {block['product_id']}"
                                            f" @ {block['last_timestamp']}"
                                        )
                                        await db.insert_pricing_data(block)
                                        info[pair] = []
                            else:
                                info[pair] = [json_response]
                            logger.debug(f"Data Recieved for pair: {pair}")
                            logger.debug(f"Size of {pair} buffer: "
                                         f"{len(info[pair])}")
                        # print(json_response) if the ws json changes

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
    p.add_argument(
        "--backend",
        "-b",
        type=str,
        default="auto",
        choices=["timescaledb", "sqlite", "auto"],
        dest="backend",
        help="Database backend: timescaledb, sqlite, or auto (default: auto)"
    )
    p.add_argument(
        "--tsdb-host",
        type=str,
        default="localhost",
        dest="tsdb_host",
        help="TimescaleDB host (default: localhost)"
    )
    p.add_argument(
        "--tsdb-port",
        type=int,
        default=5432,
        dest="tsdb_port",
        help="TimescaleDB port (default: 5432)"
    )
    p.add_argument(
        "--tsdb-name",
        type=str,
        default="coinwatchdog",
        dest="tsdb_name",
        help="TimescaleDB database name (default: coinwatchdog)"
    )
    p.add_argument(
        "--tsdb-user",
        type=str,
        default="coinwatchdog-admin",
        dest="tsdb_user",
        help="TimescaleDB username (default: coinwatchdog-admin)"
    )
    p.add_argument(
        "--tsdb-pass",
        type=str,
        default=None,
        dest="tsdb_pass",
        help="TimescaleDB password (or set COINWATCHDOG_DBPASS env var)"
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

    # Resolve password from env var if not provided via CLI
    tsdb_pass = opts.tsdb_pass or os.environ.get("COINWATCHDOG_DBPASS")
    tsdb_user = opts.tsdb_user or os.environ.get("COINWATCHDOG_DBUSER")

    db = get_backend(
        opts.backend,
        # TimescaleDB params
        host=opts.tsdb_host,
        port=opts.tsdb_port,
        dbname=opts.tsdb_name,
        user=tsdb_user,
        password=tsdb_pass,
        # SQLite fallback
        db_path=opts.database,
    )

    try:
        asyncio.run(websocket_listener(db))
    except KeyboardInterrupt:
        logger.info("Exiting WebSocket..")


def entry():
    opts = cli()
    main(opts)
