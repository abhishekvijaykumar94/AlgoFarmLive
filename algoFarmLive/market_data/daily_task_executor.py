from algoFarmAdapter.market_data.simulator.live_market_data_simulator import LiveMarketDataSimulator

from algoFarmLive.utils.property_manager import PropertyManager

if __name__ == '__main__':

    start_date_time = '2024-06-03 00:00:00'
    end_date_time = '2024-06-03 00:00:00'
    bucket_name = PropertyManager.getValue('s3bucket')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi.liveMarketData')
    daily_market_downloader = LiveMarketDataSimulator(start_date_time, end_date_time,
                                                      InfluxDBClientManager(),
                                                      bucket_name, live_market_data_kafka_topic)
    daily_market_downloader.run()

# ******************************************************************************************
#     start_dateTime = '2024-06-03 00:00:00'
#     end_dateTime = '2024-06-03 00:00:00'
#     bucket_name = PropertyManager.getValue('s3bucket')
#     dailyMarketDownloader = DailyMarketDataDownloader(start_dateTime, end_dateTime,
#                                                                                   InfluxDBClientManager(),
#                                                                                   bucket_name,100)
#     dailyMarketDownloader.run()

# ******************************************************************************************
#     start_dateTime = '2024-01-02 00:00:00'
#     end_dateTime = '2024-01-02 00:00:00'
#     dailyGreekDataConstructorVectorized =  DailyGreekDataConstructorVectorized(start_dateTime,end_dateTime,
#                                                                                InfluxDBClientManager(), "tick_market_feed")
#
#     dailyGreekDataConstructorVectorized.run()