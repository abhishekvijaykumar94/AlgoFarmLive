import os

from algoLibs.market_data_stream.connection_manager import BotoConnectionManager
from algoLibs.utils import PropertyManager, CommonUtils

if __name__ == '__main__':
    boto_connection_manager = BotoConnectionManager(PropertyManager.getValue('s3bucket'))
    system_date = CommonUtils.get_system_date_string()
    files_in_bucket = boto_connection_manager.list_files_in_bucket()
    market_data_file = 'Market_Data_' + system_date + '.7z'
    hist_mkt_data_output_dir = CommonUtils.get_hist_mkt_data_file_path_output_directory()
    output_directory_path = os.path.join(hist_mkt_data_output_dir, 'Market_Data_20240419.7z')
    boto_connection_manager.download_object('Market_Data_20240419.7z', output_directory_path)



    #Test all Boto3 Functions
    #Get File Metadata
    # metaData = botoConnnectionManager.head_object( systemDate + '/requirements1.txt')


    # Delete File if exists
    # response = botoConnnectionManager.delete_object(systemDate + '/requirements1.txt')
    # botoConnnectionManager.download_object()
    # Upload File
    # relative_path = '/Users/abhishekvijaykumar/PycharmProjects/AlgoFarm/algoFarm/requirements.txt'
    # botoConnnectionManager.upload_file(relative_path,systemDate+'/requirements1.txt')

    # Download File
    # botoConnnectionManager.download_object('20240330/requirements1.txt','requirements1.txt')