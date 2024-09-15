import sys
import algoFarmAdapter as adapter
# import algoLibs as libs
import algoLibs.utils as libutils
# import algoLibs.dao as libdao
# import algoLibs.utils as libutils
# from algoFarmAdapter.external.smart_api_connection_manager import SmartApiConnectionManager
# from algoFarmAdapter.market_data.live.kafka_producers import MockMarketDataFeeder
# from algoLibs.dao.Influx_db_client_manager import InfluxDBClientManager
import algoLibs.dao.Influx_db_client_manager as libdao
# from algoLibs.utils import PropertyManager, check_holiday, email_to_oneself

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


