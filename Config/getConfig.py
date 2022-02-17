# -*- coding: utf-8 -*-
import logging
import base64
from importlib import reload
from matplotlib import font_manager, rc
import jaydebeapi as jdb_api
from multiprocessing import Process, Lock, Queue, Manager

################################################
################################################
################################################
# 프로젝트에서 공통으로 사용하는 변수 선언
################################################
################################################
################################################
class CommonDefine:
    # ------------------------------
    # -- Server, Local 실행 여부
    # ------------------------------
    # isStartWhichInfra = "T"
    isStartWhichInfra = "F"

    # ------------------------------
    # -- Multiple Processing 여부
    # ------------------------------
    # isMultipleProcessing = True
    isMultipleProcessing = False

    # ------------------------------
    # -- Logging
    # ------------------------------
    isSetLogging = 'T'

    # ------------------------------
    # -- 실제 컨텐츠 파일패턴(READ)
    # ------------------------------
    global_indics_type = ['bank_version1', 'account']

    # ------------------------------
    # -- 색인 메모리 버퍼 사이즈
    # ------------------------------
    memory_size = 524288
    # memory_size = 1048576

    # Path = '/ES/ES_Bulk_Incre_Project/Input/Law/'
    # PATTERN = 'Initial_Training'

    Path = '/home/BULK_MIGRATION_7.9/ing/server-management/'
    PATTERN = '_elasticsearch_'

    # ------------------------------
    # -- 전체 반영 (DB -> SEARCH)
    # ------------------------------
    index_total_count = 0

    def set_index_total_count(self):
        self.index_total_count += 1

    def get_index_total_count(self):
        return self.index_total_count


################################################
################################################
################################################
# Word2Vec Define
################################################
################################################
################################################
class File:

    def __init__(self):

        if str(CommonDefine().isStartWhichInfra).__eq__('T'):
            self.root_path = '/TOM/ES/'
            self.Server_JDBC_Driver = '/TOM/ES/ES_Bulk_Incre_Project/Lib/DB/ojdbc6.jar'
            self.Server_Logging_Path = '/TOM/ES/ES_Bulk_Incre_Project/'

        else:
            self.root_path = '/ES/'
            self.Server_JDBC_Driver = '/ES/ES_Bulk_Incre_Project/Lib/DB/ojdbc6.jar'
            self.Local_Logging_Path = '/ES/ES_Bulk_Incre_Project/'


    def get_Root_Directory(self):
        return self.root_path

    def getOutputFilePath(self):
        if CommonDefine().isStartWhichInfra == "T":
            return self.Server_Logging_Path
        else:
            return self.Local_Logging_Path

    # Server_JDBC_Driver = '/TOM/ES/ES_Bulk_Incre_Project/Lib/DB/ojdbc6.jar'
    # Server_JDBC_Driver = '/ES/ES_Bulk_Incre_Project/Lib/DB/ojdbc6.jar'




################################################
################################################
################################################
# elasticsearch tokenizer api
# ip config
# URL = 'http://172.31.146.152:9200/WAS_REST_ARIRANG_TOKENIZER/_analyze/'
# URL = 'http://172.31.142.114:9200/WAS_REST_ARIRANG_TOKENIZER/_analyze/
################################################
################################################
################################################
class ElasticsearchConfig:

    ElasticsearchIP = 'x.x.x.x:9201'

    """
    def __init__(self):
        print('ElasticsearchConfig Class')
    """

    def getElasticsearchIP(self):
        return self.ElasticsearchIP
