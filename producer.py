import json

import gazu
from decouple import config
from pika import BasicProperties
from pika import BlockingConnection
from pika import ConnectionParameters

GAZU_HOST = config("GAZU_HOST")
GAZU_EVENT_HOST = config("GAZU_EVENT_HOST")
GAZU_LOGIN = config("GAZU_LOGIN")
GAZU_PW = config("GAZU_PW")
BROKER_URL = config("BROKER_URL")
BROKER_EXCHANGE_NAME = config("BROKER_EXCHANGE_NAME")

gazu.set_host(GAZU_HOST)
gazu.set_event_host(GAZU_EVENT_HOST)
gazu.log_in(GAZU_LOGIN, GAZU_PW)


def publish(key, payload):
    params = ConnectionParameters(BROKER_URL)
    cnn = BlockingConnection(params)
    channel = cnn.channel()
    channel.exchange_declare(exchange=BROKER_EXCHANGE_NAME, exchange_type='topic')
    properties = BasicProperties(delivery_mode=2)
    channel.basic_publish(exchange=BROKER_EXCHANGE_NAME,
                          routing_key=key,
                          properties=properties,
                          body=json.dumps(payload).encode())


def on_asset_created(data):
    asset_id = data["asset_id"]
    asset = gazu.asset.get_asset(asset_id)
    publish("gazu.asset.created", asset)


def on_asset_updated(data):
    asset_id = data["asset_id"]
    asset = gazu.asset.get_asset(asset_id)
    publish("gazu.asset.modified", asset)


event_client = gazu.events.init()
gazu.events.add_listener(event_client, "asset:new", on_asset_created)
gazu.events.add_listener(event_client, "asset:update", on_asset_updated)
gazu.events.run_client(event_client)
