import sys

import algoFarmAdapter as adapter
import algoLibs.dao.Influx_db_client_manager as libdao
import algoLibs.utils as libutils

if __name__ == '__main__':
    ### Check holiday for today date..If today is holiday the program will exit
    if libutils.check_holiday():
        libutils.email_to_oneself("Today is holiday..Enjoy!")
        sys.exit()

    # token_list = generate_tokens()
    api_key = libutils.PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = libutils.PropertyManager.getValue('smartApi.liveMarketData')
    bootstrap_servers = libutils.PropertyManager.getValue('bootstrap.servers')
    influx_db_client_manager = libdao.InfluxDBClientManager()
    connection_manager = adapter.SmartApiConnectionManager(api_key)
    data, feed_token = connection_manager.generate_session()
    batch_size = libutils.PropertyManager.getValue('batch_size')

    smart_api_market_data_feeder = adapter.MockMarketDataFeeder(api_key, data, live_market_data_kafka_topic, bootstrap_servers,
                                                        feed_token, int(batch_size))

    # smart_api_market_data_consumer = MarketDataConsumer(live_market_data_kafka_topic, bootstrap_servers, "smartApiLiveMarketData", influx_db_client_manager)

    smart_api_market_data_feeder.start()


