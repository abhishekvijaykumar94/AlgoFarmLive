import asyncio

from algoFarmAdapter.market_data.live.kafka_consumers import PickleMarketDataAsyncConsumer
from algoLibs.market_operations import EquityMarketDataPickleOperations
from algoLibs.utils import PropertyManager, CommonUtils

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi.liveMarketData')
    bootstrap_servers = PropertyManager.getValue('bootstrap.servers')
    pickle_output_path = CommonUtils.getFilePathOutputDirectory()
    market_data_operations = EquityMarketDataPickleOperations()
    batch_size = PropertyManager.getValue('batch_size')
    smart_api_pickle_market_data_consumer = PickleMarketDataAsyncConsumer(live_market_data_kafka_topic,
                                                                          bootstrap_servers, "smartApiLiveMarketData",
                                                                          market_data_operations, int(batch_size))
    print("Initiating Pickle Market Data Consumer on Topic:", live_market_data_kafka_topic)
    asyncio.run(smart_api_pickle_market_data_consumer.consume())


