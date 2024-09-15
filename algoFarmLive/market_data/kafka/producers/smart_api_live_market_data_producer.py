import sys

from algoFarmAdapter.external.smart_api_connection_manager import SmartApiConnectionManager
from algoFarmAdapter.market_data.live.kafka_producers import MarketDataFeeder
from algoLibs.dao import InfluxDBClientManager
from algoLibs.market_data_stream import generate_tokens
from algoLibs.utils import email_to_oneself, check_holiday, PropertyManager

if __name__ == '__main__':
    ### Check holiday for today date..If today is holiday the program will exit
    if check_holiday():
        email_to_oneself("Today is holiday..Enjoy!")
        sys.exit()

    token_list = generate_tokens()
    message = "Starting script for today for {} tickers /n".format(len(token_list))
    message += "Tokens are :"
    message += ",".join([item['tokens'][0] for item in token_list])
    email_to_oneself(message)

    api_key = PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi.liveMarketData')
    bootstrap_servers = PropertyManager.getValue('bootstrap.servers')
    influx_db_client_manager = InfluxDBClientManager()
    connection_manager = SmartApiConnectionManager(api_key)
    data, feed_token = connection_manager.generate_session()
    batch_size = PropertyManager.getValue('batch_size')

    smart_api_market_data_feeder = MarketDataFeeder(api_key, data, live_market_data_kafka_topic, bootstrap_servers,
                                                    feed_token, token_list, int(batch_size))

    # smart_api_market_data_consumer = MarketDataConsumer(live_market_data_kafka_topic, bootstrap_servers, "smartApiLiveMarketData", influx_db_client_manager)

    smart_api_market_data_feeder.start()


