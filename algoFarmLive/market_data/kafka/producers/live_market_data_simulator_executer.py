from algoFarmAdapter.market_data.simulator.live_market_data_simulator import LiveMarketDataSimulator
from algoLibs import InfluxDBClientManager, PropertyManager

if __name__ == '__main__':

    start_date_time = '2025-01-28 00:00:00'
    end_date_time = '2025-01-29 00:00:00'
    bucket_name = PropertyManager.getValue('s3bucket')
    live_market_data_kafka_topic = PropertyManager.getValue('smartApi_liveMarketData')
    daily_market_downloader = LiveMarketDataSimulator(start_date_time, end_date_time,
                                                      InfluxDBClientManager(),
                                                      bucket_name, live_market_data_kafka_topic)
    daily_market_downloader.run()