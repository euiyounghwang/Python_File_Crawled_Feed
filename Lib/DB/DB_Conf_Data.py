
import os
import datetime
import configparser
import sys

import jaydebeapi as jdb_api
import pandas as pd
import pandas.io.sql as pd_sql
from pandas import DataFrame
import pympler.asizeof
import jpype

import ES_Bulk_Incre_Project.Config.getConfig as Config
import ES_Bulk_Incre_Project.Lib.Interface.Elastic_Bulk as Bulk
import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
import ES_UnFair_Detection.WebService.Util as Utils

# sys.path.append('/TOM/ES/')
# sys.path.append('/ES/')

from ES_Bulk_Incre_Project.Lib.DB.DB_Transaction import DB_Transaction_Cls
from ES_Bulk_Incre_Project.Lib.DB.DB_Conf import *


def db_data_get_bulk_select_transaction(system_classfy, conn, params='', Is_Enable_Bulk_Options = False):
    """

    :param system_classfy:
    :param conn:
    :param params:
    :param Is_Enable_Bulk_Options:
    :return:
    """
    # conn = DB_Transaction_Cls(system_classfy).set_connection()
    try:
        print('\n\ndb_data_get_bulk_select_transaction')

        sql = get_bulk_sql_select_sql(system_classfy, Is_Enable_Bulk_Options)
        # print('sql ', sql)
        # params_colmun = ['LAW_ID', 'SUB_LAW_ID', 'INFRINGEMENT_ID', 'INFRINGEMENT_NAME']

        result_rows = []

        # df_all = pd_sql.read_sql_query(''.join(sql), conn, None)
        df_all = pd_sql.read_sql_query(''.join(sql), conn, params=params)
        # print('\n\n', df_all)
        # print('\n\n', df_all.keys(), len(df_all.keys()))

        # for column in df_all.keys():
        #     print(column)
        # import cx_Oracle
        # print('\n\n', df_all._get_values)

        # pip3 install pyhdb

        if df_all.shape[0] > 0:
            for loop in range(0, len(df_all._get_values)):
                results_dict = {}
                for column in df_all.keys():
                    # print(loop, column)
                    # print(loop, column, df_all.get(column)[loop])
                    # print(loop, df_all.get(column)[loop], df_all._get_values[loop][0])
                    results_dict[column] = str(df_all.get(column)[loop]).replace('None', '')\
                                                                        .replace('"','')\
                                                                        .replace('\r','')\
                                                                        .replace('\n','')


                result_rows.append(results_dict)
                loop += 1

        # df_temp = pd.DataFrame([df_all.get("DETECT_SENTENCE")[loop]],columns=["DETECT_SENTENCE"])
        # df=df.append(df_temp,ignore_index=True)
        # print(df)

    except Exception as ex:  # 에러 종류
         print('db_data_get_bulk_select_transaction >> ' + ex)

    # return []
    return result_rows


def db_data_get_bulk_select_rownum_transaction(OBJ_CONFIG, system_classfy, conn, ACTION_FLAG, params=[], idx=None):
    """
     # DB SELECT ROWNUM
     new_params ['30', '20200902000000', 3601, 3700]
     :param OBJ_CONFIG:
     :param system_classfy:
     :param conn: 
     :param ACTION_FLAG:
     :param params:
     :param idx:
     :return:
    """
    # elasticsearch 7.9 default '_doc'
    obj_feed = Bulk.elasticsearch_interface(idx, None)

    obj_config = Config.CommonDefine()

    try:
        print('\n\ndb_data_get_bulk_select_rownum_transaction')

        paing_row_num = 100

        start_row_num = 1
        end_row_num = paing_row_num

        new_params = []

        for input in params:
            new_params.append(input)

        # new_params.append(start_row_num)
        # new_params.append(end_row_num)

        # print('new_params', new_params)
        sql = db_config_setting().get_bulk_sql_select_sql(system_classfy)
        # print('sql ', sql)
        result_rows = []
        loop = 0
        while 1:
            print('\n' + Utils.bcolors().BOLD + Utils.bcolors().YELLOW + 'idx : ' + str(idx) + Utils.bcolors().ENDC)
            print(Utils.bcolors().BOLD + Utils.bcolors().YELLOW + 'new_params : ' + str(new_params) + Utils.bcolors().ENDC)
            print('\n')

            # df_all = pd_sql.read_sql_query(''.join(sql), conn, None)
            df_all = pd_sql.read_sql_query(''.join(sql), conn, params=new_params)

            # pip3 install pyhdb
            if df_all.shape[0] > 0:
                for loop in range(0, len(df_all._get_values)):
                    results_dict = {}
                    for column in df_all.keys():
                        # print(loop, column, df_all.get(column)[loop])
                        results_dict[column] = str(df_all.get(column)[loop]).replace('None', '')\
                                                                            .replace('"','')\
                                                                            .replace('\r','')\
                                                                            .replace('\n','')

                    print(loop, [results_dict])

                    obj_feed.bulk_add_meta([results_dict], flag=ACTION_FLAG)

                    if obj_feed.get_lists_dict_length(obj_feed.StringBuffer) > obj_feed.memory_size:
                        print('\n')
                        log.info('body length_send : ' + str(len(obj_feed.StringBuffer)) + ',\t' + str(obj_feed.get_lists_dict_length(obj_feed.StringBuffer)))
                        obj_feed.bulk_send(obj_feed.StringBuffer)
                        print('\n')
                        obj_feed.StringBuffer.clear()

                    loop += 1
                    # Check Total Rows Count
                    OBJ_CONFIG.set_index_total_count()

                new_params.clear()

                for input in params:
                    new_params.append(input)

                start_row_num += paing_row_num
                end_row_num += paing_row_num

                # new_params.append(start_row_num)
                # new_params.append(end_row_num)

            else:
                log.info('No Recored...\n')
                break

            # NO ROWNUM
            break;

        if int(obj_feed.get_lists_dict_length(obj_feed.StringBuffer) > 0):
            print('\n')
            log.info('body length_remained_send : ' + str(len(obj_feed.StringBuffer)) + ',\t' + str(obj_feed.get_lists_dict_length(obj_feed.StringBuffer)))
            obj_feed.bulk_send(obj_feed.StringBuffer)
            print('\n')
            obj_feed.StringBuffer.clear()

    except Exception as ex:  # 에러 종류
         print('db_data_get_bulk_select_transaction >> ' + ex)

    # return []
    return result_rows
