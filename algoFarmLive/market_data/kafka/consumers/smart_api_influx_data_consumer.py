import asyncio
from algoFarmAdapter.market_data.live.kafka_consumers import InfluxMarketDataAsyncConsumer
from algoLibs.dao import InfluxDBClientManager
from algoLibs.file_processors import TokenMappingProcessor
from algoLibs.market_operations.equity_market_data_influxdb_operations import EquityMarketDataInfluxDbOperations
from algoLibs.utils import PropertyManager, CommonUtils

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi.liveMarketData')
    boostrap_servers = PropertyManager.getValue('boostrap.servers')
    pickle_output_path = CommonUtils.getFilePathOutputDirectory()
    market_data_operations = EquityMarketDataInfluxDbOperations()
    batch_size=PropertyManager.getValue('batch_size')
    token_mapping_processor = TokenMappingProcessor('Token_mapping.csv')
    token_mapping_df = token_mapping_processor.get_token_to_symbol_df()
    influxDb_client_manager = InfluxDBClientManager()
    smart_api_influx_market_data_consumer = InfluxMarketDataAsyncConsumer(live_market_data_kafka_topic, boostrap_servers,
                                                                          "smartApiLiveMarketData",token_mapping_df,
                                                                          market_data_operations,influxDb_client_manager,int(batch_size))
    print("Initiating Influx Market Data Consumer on Topic:", live_market_data_kafka_topic)
    asyncio.run(smart_api_influx_market_data_consumer.consume())


