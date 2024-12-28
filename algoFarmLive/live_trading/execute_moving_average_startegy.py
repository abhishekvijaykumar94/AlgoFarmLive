import asyncio

import algoLibs as libs
from algoLibs import AngelBrokingBrokerage, NaiveAllocationStrategy, LivePortfolio, InfluxAggregationType

from algoFarmLive.live_trading.startegies.moving_average_startegy import MovingAverageStrategy

if __name__ == '__main__':

    service_name = "moving_average_start_manager"
    bootstrap_servers = libs.PropertyManager.getValue(libs.AppConstants.BOOTSTRAP_SERVERS)
    moving_average_long_window = 50
    moving_average_short_window = 20
    moving_average_stategy =  MovingAverageStrategy('MovingAverageStrategy',moving_average_short_window,moving_average_long_window)
    tickers = ["AXISBANK-EQ", "APOLLOTYRE-EQ", "SBIN-EQ", "CIPLA-EQ", "POWERGRID-EQ"]
    influx_client_manager = libs.InfluxDBClientManager()
    redis_client_manager = libs.RedisClientManager()
    capital = 100000
    angelBroking = AngelBrokingBrokerage()
    allocation_strategy = NaiveAllocationStrategy(capital, 1000.0, len(tickers))
    live_portfolio = LivePortfolio(capital,angelBroking,allocation_strategy)
    equityMarketDataQuery = libs.EquityMarketDataQuery(
        tag_string="symbol",
        tags=tickers,
        # start_time="-50m",
        start_time=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        aggregate_timeframe="1m",
        aggregation_fn=InfluxAggregationType.LAST,
        add_pivot=True)
    technicalIndicatorStrategyManager = libs.TechnicalIndicatorStrategyManager(service_name=service_name,
                                                                               kafka_bootstrap_servers=bootstrap_servers,
                                                                               strategy=moving_average_stategy,
                                                                               live_portfolio=live_portfolio,
                                                                               query=equityMarketDataQuery,
                                                                               influx_client_manager=influx_client_manager,
                                                                               redis_client_manager=redis_client_manager,
                                                                               capital=capital,
                                                                               query_executor_data=libs.QueryExecutorData(
                                                                                   "-3hr"),
                                                                               sampling_frequency=60,
                                                                               start_datetime=start_time,
                                                                               end_datetime=end_time)

    asyncio.run(technicalIndicatorStrategyManager.run())
