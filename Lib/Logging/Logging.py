# -*- coding: utf-8 -*-

import logging
import logging.handlers
from datetime import datetime
from logging.handlers import RotatingFileHandler
import ES_Bulk_Incre_Project.Config.getConfig as config


file_name = 'ES_Bulk_Incremental_Log_'

# 오늘 날짜 시 분 초 로그 저장
#today_date = datetime.today().strftime('%Y-%m-%d %H_%M_%S')
today_date = datetime.today().strftime('%Y%m%d')

# logger 인스턴스를 생성 및 로그 레벨 설정
log = logging.getLogger('TEST_LOGGER.')
log.setLevel(logging.DEBUG)

# formatter 생성
formatter = logging.Formatter('[ %(levelname)-10s | %(filename)s: %(lineno)s\t\t] %(asctime)s > %(message)s')
# formatter = logging.Formatter('[%(levelname)-s] %(asctime)s > %(message)s')

Log_File_Size = 10*1024*1024
# 스트림 / 파일 로그 출력 핸들러
#fileHandler = logging.FileHandler(config.File().getOutputFilePath() + '/Log/' + str(file_name + today_date) + '.log')
# fileHandler = RotatingFileHandler(config.File().getOutputFilePath() + '/Log/' + str(file_name + today_date) + '.log', mode='a', maxBytes=Log_File_Size, backupCount=2, encoding=None, delay=0)
fileHandler = RotatingFileHandler(config.File().getOutputFilePath() + '/Log/' + str(file_name) + '.log', maxBytes=Log_File_Size, backupCount=10, encoding=None, delay=0)
fileHandler.setFormatter(formatter)
streamHandler = logging.StreamHandler()

# 스트림 / 파일 로그 출력 핸들러 + formatter
fileHandler.setFormatter(formatter)
streamHandler.setFormatter(formatter)

# logger 인스턴스 + 핸들러
log.addHandler(fileHandler)
log.addHandler(streamHandler)


################################################
################################################
################################################
# 로그 데이터 INFO 쌓기
################################################
################################################
################################################
def info(message):
    """
    로그 데이터 INFO 쌓기
    :param message:
    :return:
    """
    if config.CommonDefine().isSetLogging.__eq__('T'):
        log.info(message)


################################################
################################################
################################################
# 로그 데이터 ERROR 쌓기
################################################
################################################
################################################
def error(message):
    """
    로그 데이터 ERROR 쌓기
    :param message:
    :return:
    """
    if config.CommonDefine().isSetLogging.__eq__('T'):
        log.error(message)


################################################
################################################
################################################
# 로그 데이터 INFO 쌓기
################################################
################################################
################################################
def info_dict(comment, dict):
    """
    로그 데이터 INFO 쌓기
    :param message:
    :return:
    """
    if config.CommonDefine().isSetLogging.__eq__('T'):
        for key, value in dict.items():
            log.info(comment + ' [' + key + ']' + str(value))

def info_list(comment, key, list):
    """
    로그 데이터 INFO 쌓기
    :param message:
    :return:
    """
    if config.CommonDefine().isSetLogging.__eq__('T'):
        log.info(comment + ' [' + key + ']' + str(list))


################################################
################################################
################################################
# 로그 데이터 ERROR 쌓기
################################################
################################################
################################################
def error_dict(comment, dict):
    """
    로그 데이터 INFO 쌓기
    :param message:
    :return:
    """
    if config.CommonDefine().isSetLogging.__eq__('T'):
        for key, value in dict.items():
            log.error(comment + ' [' + key + ']' + str(value))

