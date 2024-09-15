import argparse
import asyncio
import gc
import os
from datetime import date, timedelta, datetime
import pandas as pd
from algoLibs.dao import InfluxDBClientManager
from algoLibs.file_processors import TokenMappingProcessor
from algoLibs.market_data_stream.connection_manager import BotoConnectionManager
from algoLibs.market_operations.equity_market_data_influxdb_operations import EquityMarketDataInfluxDbOperations
from algoLibs.utils import CommonUtils, PropertyManager

pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.max_columns', None)


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def parse_args():
    parser = argparse.ArgumentParser(description='Download files from S3 based on a date range.')
    parser.add_argument('-s', '--start', help='Start date YYYY-MM-DD', type=valid_date)
    parser.add_argument('-e', '--end', help='End date YYYY-MM-DD', type=valid_date)
    return parser.parse_args()

def download_files_from_s3(start_date, end_date, bucket_name,batch_size):
    connection = BotoConnectionManager(bucket_name)
    all_files = connection.list_files_in_bucket()
    influxDBClientManager = InfluxDBClientManager()
    num_days = (end_date - start_date).days + 1
    date_list = [start_date + timedelta(days=x) for x in range(num_days)]
    date_str_list = ['Market_Data_'+d.strftime("%Y%m%d")+'.7z' for d in date_list]
    migrationTokenProcessor = TokenMappingProcessor('Token_mapping_migration.csv')
    dbfile_list = []
    # Download files
    for file_name in all_files:
        if any(date_str in file_name for date_str in date_str_list):
            try:
                histMktDataOutputDir = CommonUtils.getHistMktDataFilePathOutputDirectory()
                output_directory_path = os.path.join(histMktDataOutputDir, file_name)
                equityMarketDataInfluxDbOperations = EquityMarketDataInfluxDbOperations(histMktDataOutputDir)
                local_filename = file_name.split('/')[-1]
                local_file_path = os.path.join(histMktDataOutputDir, local_filename)
                if not os.path.exists(local_file_path):
                    print(f'Downloading {file_name} to {local_filename}')
                    connection.download_object(file_name, output_directory_path)
                    CommonUtils.extractDataFrom7zipToCurrentPath(output_directory_path, histMktDataOutputDir)
                else:
                    print(f'File {local_filename} already exists, skipping download.')
                filter_date = pd.to_datetime(file_name.split('_')[2].split('.')[0], format='%Y%m%d')
                token_symbol_map = migrationTokenProcessor.get_Migration_Token_to_Symbol_Dict(filter_date)#prepare_token_symbol_map(migration_token_df, filter_date)
                all_pkl_files = [f for f in os.listdir(equityMarketDataInfluxDbOperations.read_path) if
                                 f.endswith('.pkl')]
                file_counter = 0
                for dbfileName in all_pkl_files:
                    if dbfileName.endswith('.pkl'):
                        dbfile = equityMarketDataInfluxDbOperations.loadData('/' + dbfileName)
                        dbfile_list.extend(dbfile)
                        file_counter = file_counter + 1
                        if file_counter % batch_size == 0 or dbfileName == all_pkl_files[-1]:
                            print(f"Processed a total of {file_counter:,} pkl files for {file_name} file at {datetime.utcnow().time()}")
                            ticks_df = pd.DataFrame(dbfile_list)
                            ticks_df['token_int'] = ticks_df['token'].astype(int)
                            # Map symbols from pre-prepared map
                            ticks_df['symbol'] = ticks_df['token_int'].map(token_symbol_map)
                            # Handle missing symbols (if any)
                            missing_symbols = ticks_df[ticks_df['symbol'].isnull()]['token_int'].unique()
                            print(f"Completed Pre Processing of data {datetime.utcnow().time()}")
                            if len(missing_symbols) > 0:
                                print(f"Missing symbols for tokens: {missing_symbols}")
                            points = equityMarketDataInfluxDbOperations.extract_features_vectorized(ticks_df)
                            print(f"Completed Feature Extraction of data at {datetime.utcnow().time()}")
                            asyncio.run(influxDBClientManager.write_data_async(points))
                            print(f"Completed Saving Data to Influx at {datetime.utcnow().time()}")
                            del ticks_df, dbfile_list
                            gc.collect()
                            dbfile_list = []
            except Exception as e:
                print("Error while processing Data for file ", file_name)
                CommonUtils.logErrorDetails(e)
            CommonUtils.clearAllDataFromPath(CommonUtils.getHistMktDataFilePathOutputDirectory())

def  main():
    args = parse_args()
    args.start = datetime.strptime('2024-05-28', "%Y-%m-%d").date()
    args.end = datetime.strptime('2024-05-28', "%Y-%m-%d").date()
    start_date = args.start if args.start else date.today()
    end_date = args.end if args.end else date.today()
    bucket_name = PropertyManager.getValue('s3bucket')
    download_files_from_s3(start_date, end_date, bucket_name,250)


if __name__ == "__main__":
    main()
