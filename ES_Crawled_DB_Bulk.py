# -*- coding: utf-8 -*-

import sys
import os
import schedule
from multiprocessing import Process, Lock, Queue, Manager, Pool
import jpype

# sys.path.append(os.getcwd())
sys.path.append("/ES/")
# sys.path.append("/TOM/ES/")
# sys.path.append("D://POSCOICT/Project/Source/Python/")

# /TOM/Python_Install/bin/python3.5 /TOM/ES/ES_Bulk_Incre_Project/ES_Crawled_DB_Bulk.py

QUEUE_RUN = 'RUN'
QUEUE_ACT = 'INSERT'

import datetime
import time
import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
import ES_Bulk_Incre_Project.Lib.FileIO.Import_DataSet as imported
import ES_Bulk_Incre_Project.Config.getConfig as Config
import ES_Bulk_Incre_Project.Lib.Interface.Directory_Utils as Directory_Util
import ES_UnFair_Detection.WebService.Util as Utils


def init_jvm(jvmpath=None, args=None):
    """

    :param jvmpath:
    :param args:
    :return:
    """
    if jpype.isJVMStarted():
        return
    jpype.startJVM(jpype.getDefaultJVMPath(), args)


def main(isTimer, SYSTEM_ID, CONNECT_DB_PROD, ACTION_FLAG):
    """
    import ES_Bulk_Incre_Project.Lib.Interface.Elastic_Bulk as Bulk
    indics_type = ['bank_version1', 'account']
    # delte_data = [{'KEY': 'piCfEXQBXUqa8NCrfn_u'}, {'KEY': 'xSBLFXQB2uAYsLae85XL'}, {'KEY': '8CCeEXQBXUqa8NCre34A'}]
    Bulk.elasticsearch_interface(indics_type[0], indics_type[1]).bulk_add_buffer(result_sets, flag='DELETE')
    log.info('result_sets ' + str(len(result_sets)))

    # START_DATE = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d") + time.strftime('%H%M%S')
    # END_DATE = (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y%m%d") + time.strftime('%H%M%S')

    :param isTimer:
    :param SYSTEM_ID:
    :param CONNECT_DB_PROD:
    :param ACTION_FLAG: 
    :return: 
    """
    start_time = datetime.datetime.now()

    log.info("#####################################")
    # log.info('StartTime ' + str(start_time))
    print('\n\nDB Crawleed Bulk/Incre Index Starting...')

    import ES_Bulk_Incre_Project.Lib.DB.DB_Conf_Data as get_rows
    import ES_Bulk_Incre_Project.Lib.DB.DB_Transaction as Conn

    try:

        # jar = Config.File().get_Root_Directory() + '/ES_Bulk_Incre_Project/Lib/DB/ojdbc6.jar'
        jar = Config.File().Server_JDBC_Driver
        args = '-Djava.class.path=%s' % jar

        jvm_path = jpype.getDefaultJVMPath()
        # jpype.startJVM(jvm_path, args)
        init_jvm(jvm_path, args)

        conn = Conn.DB_Transaction_Cls(SYSTEM_ID, CONNECT_DB_PROD).set_connection()

        # BULK
        # START_DATE = '20210301000000'
        # START_DATE = '20200917000000'
        # START_DATE = '20201101000000'

        # INCRE
        START_DATE = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d") + '000000'

        import json
        with open(Config.File().get_Root_Directory() + '/ES_Bulk_Incre_Project/Lib/Feed_Config/INDICES.json', 'r+', encoding='utf-8') as f:
            mapping_dict = json.load(f)

        # mapping_dict = {
        #     "RUN": {
        #         "DELETE": {
        #             "01": [
        #                 "ict_ecm_grp_idx"
        #             ]
        #         }
        #     }
        # }

        OBJ_CONFIG = Config.CommonDefine()

        # ACTION_FLAG[0] : RUN
        # ACTION_FLAG[1] : NSERT
        for COMPANY_CODE, IDX_LIST in mapping_dict[ACTION_FLAG[0]][ACTION_FLAG[1]].items():
            for idx in IDX_LIST:
                print('\n' + Utils.bcolors().BOLD + Utils.bcolors().YELLOW + str(ACTION_FLAG[0]) + '\t' + str(ACTION_FLAG[1]) + Utils.bcolors().ENDC)
                print(Utils.bcolors().BOLD + Utils.bcolors().YELLOW +  str(COMPANY_CODE) + '\t' + str(idx) + Utils.bcolors().ENDC)

                # RUN, INSERT -> QUEUE_HISTORY_IDX YYYY-MM
                if ACTION_FLAG[0].__eq__(QUEUE_RUN) and ACTION_FLAG[1].__eq__(QUEUE_ACT):
                    idx = idx + '_' + (datetime.date.today() - datetime.timedelta(days=0)).strftime("%Y-%m")

                print('idx', idx)
                params = [COMPANY_CODE, START_DATE]
                # params = []
                # ACTION_FLAG[1] 'INSERT' or 'DELETE'
                result_sets = get_rows.db_data_get_bulk_select_rownum_transaction(OBJ_CONFIG, SYSTEM_ID, conn, ACTION_FLAG[1], params, idx)

    finally:
        Conn.DB_Transaction_Cls(SYSTEM_ID, CONNECT_DB_PROD).set_disconnection(conn)

    end_time = datetime.datetime.now()

    print(Utils.bcolors().BOLD + Utils.bcolors().YELLOW + 'Total Transaction Query Count >> {}'.format(format(OBJ_CONFIG.get_index_total_count(), ",")) + Utils.bcolors().ENDC)

    print('\n')
    print('#'*50)
    print('Analyzter Start Time >> {}\nAnalyzter End Time >> {}'.format(start_time, end_time))
    print('Analyzter RunningTime >> {}'.format(end_time - start_time))
    print('#'*50)
    print('\n')

    # ['/ES/ES_Bulk_Incre_Project/ES_Crawled_DB_Bulk.py', 'aa', 'bb', 'cc']
    # print(sys.argv)

    if isTimer.__eq__('T'):
        status = '[FEED DELETE INSERT INCRE_' + str(SYSTEM_ID).upper() + ']'
        print('\n' + Utils.bcolors().BOLD + Utils.bcolors().YELLOW + status + ' Feed Migration Scheduling Start Time >> {}'.format(datetime.datetime.now()))


if __name__ == '__main__':

    isTimer = 'T'
    # isTimer = 'F'

    CONNECT_DB_PROD = True
    # CONNECT_DB_PROD = False

    # ACTION_FLAG = ['DEBUG', 'INSERT']
    # ACTION_FLAG = ['DEBUG', 'DELETE']
    ACTION_FLAG = ['RUN', 'INSERT']
    # ACTION_FLAG = ['RUN', 'DELETE']

    SYSTEM_ID = 'ECM'

    if isTimer.__eq__('T'):
        # schedule.every(10).seconds.do(main, company_code, system_id, is_enabl_bulk_options)
        # schedule.every(10).minutes.do(main, company_code, system_id, is_enabl_bulk_options)
        # schedule.every().hour.do(main, SYSTEM_ID, CONNECT_DB_PROD, ACTION_FLAG)
        # schedule.every().day.at("201908:10").do(main, company_code,  system_id, is_enabl_bulk_options)
        schedule.every().day.at("23:00").do(main, isTimer, SYSTEM_ID, CONNECT_DB_PROD, ACTION_FLAG)
        status = ''

        status = '[FEED DELETE INSERT INCRE_' + str(SYSTEM_ID).upper() + ']'
        print('\n' + Utils.bcolors().BOLD + Utils.bcolors().YELLOW + status + ' Feed Migration Scheduling Start Time >> {}'.format(datetime.datetime.now()))

        while 1:
            schedule.run_pending()

            # status = '[FEED DELETE INSERT INCRE_' + str(SYSTEM_ID).upper() + ']'
            # print('\n' + Utils.bcolors().BOLD + Utils.bcolors().YELLOW + status + ' Feed Migration Scheduling Start Time >> {}'.format(datetime.datetime.now()))
            time.sleep(1)
            # time.sleep(1800)
            # time.sleep(600)
            # time.sleep(10)

    else:
        main(isTimer, SYSTEM_ID, CONNECT_DB_PROD, ACTION_FLAG)


