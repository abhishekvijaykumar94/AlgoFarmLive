import asyncio

from algoFarmAdapter.market_data.live.kafka_consumers import LiveMarketDataConsumer
from algoLibs import PropertyManager, TickerSubscriptionQuery, QueryExecutorData, TickerSubscription, DataRepository
from algoLibs.converters.equity_market_data_converter import EquityMarketDataConverter
from algoLibs.utils import CommonUtils

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi_liveMarketData')
    bootstrap_servers = PropertyManager.getValue('boostrap_servers')
    pickle_output_path = CommonUtils.get_file_path_output_directory()
    equity_market_data_converter = EquityMarketDataConverter()
    data_repository = DataRepository()
    batch_size = PropertyManager.getValue('batch_size')
    ticker_subscription_query = TickerSubscriptionQuery(max_timestamp=True)
    query_executor_data = QueryExecutorData("-3hr")
    ticker_subscriptions: list[TickerSubscription] = data_repository.query(ticker_subscription_query,
                                                                           query_executor_data)
    token_symbol_map = {}
    for ticker_subscription in ticker_subscriptions:
        token_symbol_map[ticker_subscription.token] = ticker_subscription.ticker_symbol

    smart_api_influx_market_data_consumer = LiveMarketDataConsumer(kafka_topic=live_market_data_kafka_topic,
                                                                       bootstrap_servers=bootstrap_servers,
                                                                       kafka_group_id="smartApiLiveMarketData",
                                                                       token_symbol_map=token_symbol_map,
                                                                       retention_seconds=10800,
                                                                       batch_size=int(batch_size),
                                                                       persist_to_cache=False,
                                                                       persist_to_influx=True
                                                                       )
    print("Initiating Influx Market Data Consumer on Topic:", live_market_data_kafka_topic)
    asyncio.run(smart_api_influx_market_data_consumer.consume())



