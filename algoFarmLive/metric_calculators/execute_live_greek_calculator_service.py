from algoLibs.dao import DataRepository
from algoLibs.file_processors import TokenMappingProcessor
from algoLibs.live_trading import LiveGreekCalculatorService
from algoLibs.market_operations import EquityMarketDataPickleOperations, EqOptionGreeksDataInfluxOperations
from algoLibs.utils import PropertyManager, CommonUtils

if __name__ == '__main__':
    api_key = PropertyManager.getValue('apikey')
    greek_calculator_kafka_topic = PropertyManager.getValue('smartApi.greekCalculator')
    bootstrap_servers = PropertyManager.getValue('boostrap.servers')
    pickle_output_path = CommonUtils.getFilePathOutputDirectory()
    market_data_operations = EquityMarketDataPickleOperations()
    batch_size = PropertyManager.getValue('batch_size')

    token_processor = TokenMappingProcessor('Token_mapping.csv',
                                            CommonUtils.getFilePathFromDataDirectory('Token_mapping.csv'))
    derivative_token_list = token_processor.get_option_tokens()
    underlying_token_list = token_processor.get_index_underlying_tokens()
    token_symbol_map = token_processor.get_token_to_symbol_dict()
    market_data_operations = EqOptionGreeksDataInfluxOperations()
    data_repository = DataRepository()
    live_greek_calculator_service = LiveGreekCalculatorService(derivative_token_list, underlying_token_list,
                                                               token_symbol_map, data_repository, "tick_market_feed")
    print("Initiating Pickle Market Data Consumer on Topic:", greek_calculator_kafka_topic)
    live_greek_calculator_service.start_greek_calculator()

    # liveGreekCalculatorService.start_greek_calculator()

