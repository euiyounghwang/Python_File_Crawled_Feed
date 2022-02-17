# -*- coding: utf-8 -*-

import sys
import os
from multiprocessing import Process, Lock, Queue, Manager, Pool

# sys.path.append(os.getcwd())
sys.path.append("/ES/")
# sys.path.append("D://POSCOICT/Project/Source/Python/")

# import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
from ES_Bulk_Incre_Project.Lib.Logging.Logging import *
import ES_Bulk_Incre_Project.Lib.FileIO.Import_DataSet as imported
import ES_Bulk_Incre_Project.Config.getConfig as Config
import ES_Bulk_Incre_Project.Lib.Interface.Directory_Utils as Directory_Util
import datetime

def main():
        '''
        RUN ~~
        python3.5 ES_Analyzer_Project/Word_Analysis_Basic.py
        curl -XPOST 'localhost:9200/bank/account/_bulk?pretty' --data-binary "@accounts.json"
        if len(sys.argv) is 1:
            print('\n#######################################################')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################')
            logging.Logging().setLogining().error('Please type elasticsearch or twitter using term analysis')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################\n')
        else:
            params = sys.argv[1]
            print('\n#######################################################')
            print('#########################################################')
            print('#########################################################')
            print(params)
            print('#########################################################')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################')
            print('#########################################################\n')
        '''

        StartTime = datetime.datetime.now()

        # log.info("#####################################")
        print('\n')
        # log.info('StartTime ' + str(start_time))
        log.info('- Elasticsearch Bulk/Incre Index Starting...')

        # is_Delete_All_Flag = True
        is_Delete_All_Flag = False

        # 실제 폴더명에서 index_name read
        # Elasticsearch Object Create
        # obj = Bulk.elasticsearch_interface(real_leaf_folder_name, Config.CommonDefine().global_indics_type[1])
        # obj = Bulk.elasticsearch_interface(Config.CommonDefine().global_indics_type[0], Config.CommonDefine().global_indics_type[1])

        if Config.CommonDefine().isMultipleProcessing:
                # sub_dir_info = Directory_Util.search_subdir(Config.CommonDefine().Path)
                sub_dir_info = Directory_Util.os_walk_searchdir(Config.CommonDefine().Path)
                # print('\n')
                # print(sub_dir_info)
                # sub_dir_info = ['ict_ecm_grp_idx', 'pnr_ecm_grp_idx']
                for folder_name in sub_dir_info:
                        # if 'posco_ecm_grp2' in folder_count:
                        # file_list = get_all_files(folder_name)
                        # print('file list ', file_list)
                        proc_bulk_list = []
                        # p = Process(target=imported.import_JSON_data_set, args=(obj, folder_name,is_Delete_All_Flag))
                        p = Process(target=imported.import_JSON_data_set, args=(folder_name, is_Delete_All_Flag))
                        proc_bulk_list.append(p)
                        p.start()

                for proc in proc_bulk_list:
                        proc.join()

        else:
                # sub_dir_info = Directory_Util.search_subdir(Config.CommonDefine().Path)
                sub_dir_info = Directory_Util.os_walk_searchdir(Config.CommonDefine().Path)

                # print('\n')
                log.info(sub_dir_info)

                if sub_dir_info:
                        for folder_name in sub_dir_info:
                                # JSON 데이터셋 색인 (LAW)
                                # imported.import_JSON_data_set(obj, folder_name,is_Delete_All_Flag)
                                imported.import_JSON_data_set(folder_name, is_Delete_All_Flag)
                                # imported.import_outsite_excel_data_set()

        EndTime = datetime.datetime.now()
        log.info('-Elasticsearch Bulk/Incre Index Finished...>> {} )'.format(EndTime - StartTime))

if __name__ == '__main__':
    main()


