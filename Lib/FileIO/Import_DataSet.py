import os
import json
import random
import re

import ES_Bulk_Incre_Project.Lib.Interface.Elastic_Bulk as Bulk
import ES_Bulk_Incre_Project.Lib.Util.Util as Utils
import ES_Bulk_Incre_Project.Config.getConfig as Config
import pandas as pd
# import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
from ES_Bulk_Incre_Project.Lib.Logging.Logging import *
import datetime


def xls_key_check(row):
    if str(row).__eq__('nan'):
        return ''
    else:
        return row


# noinspection PyIncorrectDocstring,PyShadowingNames
def import_outsite_excel_data_set():
    '''
    data = pd.read_html('/ES/ES_UnFair_Detection/Input/Train/Patent/Files/car_material.xls')
    # print(data)

    df = data[0]
    print(df.columns)
    print(len(df[5]))
    '''
    Path = '/ES/ES_Bulk_Incre_Project/Input/Law/'
    data_set_list = []
    row = {}
    for filename in os.listdir(Path):
        # result_r = re.search('_Test_Set.xlsx', filename)
        result_r = re.search('_TrainSet.xlsx', filename)
        if result_r:
            print('result_r ' + str(filename))
            data = pd.read_excel(Path + str(filename))
            for i in range(0, len(data)):
                row['law'] = '부당특약'
                row['from_source'] = xls_key_check(str(data['법률구분'][i]))
                row['train_source'] = xls_key_check(str(data['출 처'][i]))
                row['train_detail_source'] = xls_key_check(str(data['상세 출처'][i]))
                row['from_source_url'] = xls_key_check(str(data['URL'][i]))
                row['category'] = ''
                row['sentence'] = xls_key_check(str(data['공정 조항'][i]))
                row['label'] = 'POS'
                data_set_list.append(row)

    print('\n\n')
    # print(data_set_list)
    # print(json.dumps(data_set_list, indent = 2, ensure_ascii=False))

    obj.bulk_add_buffer(data_set_list, 'INSERT')


# noinspection PyIncorrectDocstring,PyUnusedLocal
def import_JSON_data_set(full_path, is_Delete_All_Flag=False):
    """
    특정디렉토리내에 있는 법률문구, 수작업된 labeled 컨텐츠 읽기
    :param obj:
    :param full_path:
    :param is_Delete_All_Flag:
    :return:
    """

    start_time = datetime.datetime.now()

    # print('\n\n')
    log.info('Import_JSON_data_set folder_name [1] >> ' + full_path)
    real_leaf_folder_name = str(full_path).split('/')[len(str(full_path).split('/'))-1]
    log.info('Import_JSON_data_set folder_name [2] >> ' + real_leaf_folder_name)
    # print('\n\n')

    print(Utils.bcolors().BOLD + Utils.bcolors().YELLOW)
    log.info('source index"s name >> ' + real_leaf_folder_name)
    print(Utils.bcolors().ENDC)
    # exit()

    if str(real_leaf_folder_name).__eq__('new_e_hr'):
        real_leaf_folder_name = 'test_e_hr'

    elif str(real_leaf_folder_name).__eq__('server-management'):
        real_leaf_folder_name = 'server-management-test'

    log.info('real index"s name >> ' + real_leaf_folder_name)
    # exit(1)

    obj = Bulk.elasticsearch_interface(real_leaf_folder_name, Config.CommonDefine().global_indics_type[1])

    # 삭제 (인덱스 내 데이터)
    if is_Delete_All_Flag:
        obj.bulk_delete_bu_query()

    current_file_count = 0
    total_file_count = 0

    for filename in os.listdir(full_path):
        result_r = re.search(Config.CommonDefine().PATTERN, filename)
        if result_r:
            total_file_count += 1

    for filename in os.listdir(full_path):
        result_r = re.search(Config.CommonDefine().PATTERN, filename)
        if result_r:
            processing = []
            print('\n')
            log.info('@@@result_r@@@' + str(filename))
            current_file_count += 1

            with open(full_path + '/' + filename, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                # print(data)
                processing = [str(current_file_count), str(total_file_count)]

                # obj.bulk_add_buffer(data, processing, 'INSERT')
                obj.bulk_add_buffer(data, processing, 'UPDATE')
                # obj.bulk_add_buffer(data, processing, 'DELETE')

    # print('\n')
    log.info('Imported & Indexing the DataSet ..')
    # print('\n')
    # print(initial_test_list)

    end_time = datetime.datetime.now()

    # log.info('EndTime ' + str(end_time))
    # log.info("#####################################")
    # print('\n')
    log.info('Analyzter Start Time >> {}'.format(start_time))
    log.info('Analyzter End Time >> {}'.format(end_time))
    log.info('Analyzter RunningTime >> {}\n'.format(end_time - start_time))
    # print('\n')


# noinspection PyIncorrectDocstring,PyUnusedLocal
def import_JSON_data_sample_set(data):
    """
    특정디렉토리내에 있는 법률문구, 수작업된 labeled 컨텐츠 읽기

    :return:
    """
    initial_all_list = []
    initial_train_list = []
    initial_category_list = []
    initial_test_list = []
    initial_test_category_list = []

    Path = '/ES/ES_Bulk_Incre_Project/Input/Law/'

    obj = Bulk.elasticsearch_interface(Config.CommonDefine().global_indics_type[0], Config.CommonDefine().global_indics_type[1])

    obj.bulk_delete_bu_query()

    # 전체학습 시 가장 작은 데이터셋 기준의 length 추출
    obj.bulk_add_buffer(data, 'INSERT')

    print('\n\nImported & Indexing the DataSet ..')
    print('\n')
    # print(initial_test_list)


if __name__ == '__main__':


    # sample JSON FEEDING
    sample_data = [{'law': '부당특약', 'category': '기상여건으로 인한 비용부담 전가', 'label': 'neg', 'train_detail_source': '부당특약', 'from_source_url': '', 'from_source': 'SOURCE', 'sentence': '5.8.20 기타 이외의 사항', 'train_source': 'SOURCE'}]
    import_JSON_data_sample_set(sample_data)
    # import_JSON_data_set()
    # import_outsite_excel_data_set()





