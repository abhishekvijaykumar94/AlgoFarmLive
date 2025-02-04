import asyncio
import algoLibs as libs
from algoFarmAdapter.risk_management.post_trade_risk_management import PostTradeRiskService
from algoLibs import PropertyManager, \
    TickerSubscriptionQuery, DataRepository, TickerSubscription
from algoLibs.live_trading.events.events import EventType

if __name__ == '__main__':

    service_name = "PostTradeRiskService"
    bootstrap_servers = libs.PropertyManager.getValue(libs.AppConstants.BOOTSTRAP_SERVERS)
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi_liveMarketData')
    fill_event = EventType.Fill_Event.name
    data_repository = DataRepository()

    ticker_subscription_query = TickerSubscriptionQuery(max_timestamp=True)
    query_executor_data = libs.QueryExecutorData("-3hr")
    ticker_subscriptions:list[TickerSubscription] = data_repository.query(ticker_subscription_query,query_executor_data)
    token_symbol_map = {}
    for ticker_subscription in ticker_subscriptions:
        token_symbol_map[ticker_subscription.token] = ticker_subscription.ticker_symbol
    print("topics to subscribe to ",[live_market_data_kafka_topic,fill_event])
    post_trade_risk_service = PostTradeRiskService(service_name=service_name,
                                                   kafka_bootstrap_servers=bootstrap_servers,
                                                   consume_topics=[live_market_data_kafka_topic,fill_event],
                                                   token_symbol_map=token_symbol_map
                                                   )

    asyncio.run(post_trade_risk_service.run())