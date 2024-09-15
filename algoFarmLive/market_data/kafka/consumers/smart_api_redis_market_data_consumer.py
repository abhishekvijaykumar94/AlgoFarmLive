import asyncio

from algoFarmAdapter.market_data.live.kafka_consumers import LiveMarketDataRedisConsumer
from algoLibs.converters.equity_market_data_converter import EquityMarketDataConverter
from algoLibs.file_processors import TokenMappingProcessor

from algoLibs.utils import  CommonUtils
from algoLibs.utils.property_manager import PropertyManager

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi.liveMarketData')
    bootstrap_servers = PropertyManager.getValue('bootstrap.servers')
    pickle_output_path = CommonUtils.get_file_path_output_directory()
    equity_market_data_converter = EquityMarketDataConverter()
    batch_size = PropertyManager.getValue('batch_size')
    token_processor = TokenMappingProcessor('Token_mapping.csv',
                                            CommonUtils.get_file_path_from_data_directory('Token_mapping.csv'))
    token_symbol_map = token_processor.get_token_to_symbol_dict()
    smart_api_redis_market_data_consumer = LiveMarketDataRedisConsumer(live_market_data_kafka_topic, bootstrap_servers,
                                                                      "smartApiLiveMarketData", equity_market_data_converter,
                                                                      token_symbol_map,
                                                                      retention_seconds=10800, batch_size=int(batch_size))
    print("Initiating Redis Market Data Consumer on Topic:", live_market_data_kafka_topic)
    asyncio.run(smart_api_redis_market_data_consumer.consume())



