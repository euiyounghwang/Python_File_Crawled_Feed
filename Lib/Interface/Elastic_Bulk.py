
# http://asuraiv.blogspot.com/2015/04/elasticsearch-versionconflictengineexce.html
# shell script

import elasticsearch
import pprint
import os
import json
import datetime
from elasticsearch import helpers
import sys

# sys.path.append(os.getcwd())
sys.path.append("/ES/")
# sys.path.append("/TOM/ES/")

# /TOM/Python_Install/bin/python3.5 /TOM/ES/ES_Bulk_Incre_Project/ES_Crawled_DB_Bulk.py

import ES_Bulk_Incre_Project.Config.getConfig as Config
import ES_Bulk_Incre_Project.Lib.Interface.Query as Query
import ES_Bulk_Incre_Project.Lib.Interface.Elastic_Utils as Utils
import ES_Bulk_Incre_Project.Lib.Util.Util as Util
# import ES_Bulk_Incre_Project.Lib.Logging.Logging as log
from ES_Bulk_Incre_Project.Lib.Logging.Logging import *
import datetime

# noinspection PyMethodMayBeStatic,PyMethodParameters
class elasticsearch_interface:
    """
    https://programtalk.com/python-examples/elasticsearch.helpers.bulk/
    """

    def __init__(self, index_name, doc_type):
        # print('\n')
        # print('__init__(self)')
        self.elasticsearch_ip = Config.ElasticsearchConfig().getElasticsearchIP()

        self.index_name = index_name
        # self.doc_type = doc_type
        self.doc_type = '_doc'

        self.StringBuffer = []
        self.memory_size = Config.CommonDefine().memory_size

        log.info('self.index_name >> ' + str(self.index_name))
        log.info('self.doctype >> ' + str(self.doc_type))
        log.info('self.elasticsearch_ip >> ' + str(self.elasticsearch_ip))

        self.header = {'Content-Type': 'application/json', 'Authorization': 'Basic ZWxhc3RpYzpnc2FhZG1pbg==',
                       'Connection': 'close'}

        self.es_client = None

        self.total, self.sucess, self.fail = 0, 0, 0


    def elastic_client_connect(self):
        self.es_client = elasticsearch.Elasticsearch(self.elasticsearch_ip,
                                                     # http_auth=('elastic', 'gsaadmin'),
                                                     headers=self.header,
                                                     timeout=10000
                                                     )

    def elastic_client_close(self):
        self.es_client.transport.connection_pool.close()


    def get_cat_aliase(self):
        """
        es.cat.indices(h='index', s='index').split()
        es.indices.get_alias("*")
        :return:
        """
        try:
            # doc = self.es_client.cat.aliases(format(json))
            # doc = self.es_client.indices.get_alias("*")

            self.elastic_client_connect()

            doc = self.es_client.cat.indices({"format": "json"})
            print('\n\n')
            print('## OUTPUT (get_cat_aliase) ##')
            print(json.dumps(doc, indent=2, ensure_ascii=False))

            # self.elasticsearch_get_search()

        except Exception as ex:  # 에러 종류
            print(ex)
            pass

        finally:
            self.elastic_client_close()


    def bulk_delete_bu_query(self):
        try:
            self.elastic_client_connect()

            doc = self.es_client.delete_by_query(
                               index = self.index_name,
                               doc_type=self.doc_type,
                               conflicts = 'proceed',
                               # refresh='wait_for',
                               wait_for_completion=True,
                               body = Query.elastic_query_match_all_search()
                            )
            print('\n\n')
            print('## OUTPUT (Delete By Query) ##')
            print(json.dumps(doc, indent = 2, ensure_ascii=False))

            # self.elasticsearch_get_search()

        except Exception as ex:  # 에러 종류
            print(ex)
            pass

        finally:
            self.elastic_client_close()



    def elasticsearch_get_search(self):
        try:
             self.elastic_client_connect()
             # doc = es_client.get(index = 'ict_ecm_grp_idx', doc_type = 'ict', id = 'doc0900bf4b9814ed4a')
             # doc = es_client.get(index = 'bank_version1', doc_type = 'account', id = 'new_id_1')

             # es_client.delete(index='bank_version1', doc_type='account', id='new_id_1')
             doc = self.es_client.search(index = self.index_name,
                              doc_type = self.doc_type,
                              body = Query.elastic_query_match_all_search()
                             )
             # print('\n\n')
             # pprint.pprint(doc)
             print('\n\n')
             print('## OUTPUT (Search) ##')
             print(json.dumps(doc, indent = 2, ensure_ascii=False))

        finally:
            self.elastic_client_close


    def get_lists_dict_length(self, docs):
        total_size = 0
        for token in docs:
            total_size += len(str(token))

        return total_size


    def bulk_send(self, docs):
        """

        :param docs:
        :return:
        """

        # print('\nbulk_send1', docs)

        try:
            self.elastic_client_connect()

            success, failed, send_buffer_response = elasticsearch.helpers.custom_bulk(self.es_client, docs, refresh=True)
            # log.info(docs)
            log.info('bulk_send\t' + str(success )+ '\t' + str(failed))
            feed_success_total_count, feed_fail_total_count, feed_sum_indexing_count = Utils.elasticsearch_response(send_buffer_response)

            self.total += feed_sum_indexing_count
            self.sucess += feed_success_total_count
            self.fail += feed_fail_total_count

            print('\n')
            print(Util.bcolors().BOLD)
            log.info('#' * 40)
            log.info('#' * 40)
            log.info('Bulk Indexing Target >> {}'.format(self.index_name))
            log.info('Bulk Indexing OneTime Size >> {}'.format(format(feed_sum_indexing_count, ",")))
            log.info('Bulk Indexing Success Size >> {}'.format(format(self.sucess, ",")))
            log.info('Bulk Indexing Faild Size >> {}'.format(format(self.fail, ",")))
            log.info('Bulk Indexing Total Size >> {}'.format(format(self.total, ",")))
            log.info('#' * 40)
            log.info('#' * 40)
            print(Util.bcolors().BOLD)
            print(Util.bcolors().ENDC)

        except Exception as ex:  # 에러 종류
            log.info(ex.args[0])
            # log.info(ex)
            # print('\nerror',json.dumps(ex.args[1], indent=2, ensure_ascii=False))
            # error_list = json.dumps(ex.args[1], indent=0, ensure_ascii=False)
            # Utils.elasticsearch_response(error_list)
            # results = json.dumps(ex.args[1], indent=2, ensure_ascii=False)
            pass

        finally:
            self.elastic_client_close()



    # noinspection PyShadowingNames
    # function 아래에서 메모리 처리 안함
    def bulk_add_meta(self, data, processing=None, flag='INSERT'):
        """
        # function 아래에서 메모리 처리 모두 함
           # '_source': {
                    #     'Law': '부당특약',
                    #     'CATEGORY': rows['category'],
                    #     'TRAIN_SOURCE': rows['train_source'],
                    #     'TRAIN_DETAIL_SOURCE': rows['train_detail_source'],
                    #     'FROM_SOURCE': rows['from_source'],
                    #     'FROM_SOURCE_URL': rows['from_source_url'],
                    #     'LABEL': str(rows['label']).upper(),
                    #     'SENTENCE': rows['sentence'],
                    #     'INPUTDATE': nowDatetime
        #   }
        :param data:
        :param processing:
        :param flag:
        :return:
        """
        now = datetime.datetime.now()
        nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')

        # for cnt in range(10):
        loop = 1
        for rows in data:
            if flag.__eq__('INSERT'):
                if 'KEY' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['KEY']),
                        '_source': dict(rows)
                    }
                elif 'DOCID' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['DOCID']),
                        '_source': dict(rows)
                }

                elif 'EMPCD' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['EMPCD']),
                        '_source': dict(rows)
                }
                elif 'ES_EMP_CD' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['ES_EMP_CD']),
                        '_source': dict(rows)
                }
                else:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_source': dict(rows)
                    }

            elif flag.__eq__('DELETE'):
                 # print('@@@@@@@@DELETE')
                 body = {
                    '_op_type': 'delete',
                    '_index': self.index_name,
                    '_type': self.doc_type,
                     '_id': str(rows['KEY']),
                    # '_id': 'new_id_' + str(loop)
                 }

            elif flag.__eq__('UPDATE'):
                 body = {
                    '_op_type': 'update',
                    '_index': self.index_name,
                    '_type': self.doc_type,
                     '_id': str(rows['KEY']),
                     # '_id': str(rows['PRE_FIX']) + "_" + str(rows['KEY']),
                    # '_id': 'new_id_' + str(loop),
                    'doc': dict(rows),
                    'doc_as_upsert': False
                   }


            # print('\n\nbody ', dict(body))

            self.StringBuffer.append(dict(body))

            print('\n\nbody length ok', self.memory_size, self.get_lists_dict_length(self.StringBuffer))

            loop += 1



    # noinspection PyShadowingNames
    # function 아래에서 메모리 처리 모두 함
    def bulk_add_buffer(self, data, processing=None, flag='INSERT'):
        """
            # function 아래에서 메모리 처리 모두 함
           # '_source': {
                    #     'Law': '부당특약',
                    #     'CATEGORY': rows['category'],
                    #     'TRAIN_SOURCE': rows['train_source'],
                    #     'TRAIN_DETAIL_SOURCE': rows['train_detail_source'],
                    #     'FROM_SOURCE': rows['from_source'],
                    #     'FROM_SOURCE_URL': rows['from_source_url'],
                    #     'LABEL': str(rows['label']).upper(),
                    #     'SENTENCE': rows['sentence'],
                    #     'INPUTDATE': nowDatetime
           #   }
        :param data:
        :param processing:
        :param flag:
        :return:
        """
        docs = []

        now = datetime.datetime.now()
        nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')

        # for cnt in range(10):
        loop = 1
        for rows in data:
            # print('###', str(loop))
            # rows['PRE_FIX'] = 'PATT_BASE'
            # *****************************************
            # *****************************************
            if 'CONTENT' in data:       rows['CONTENT'] = str(rows['CONTENT']).replace('\\s+', ' ')
            if 'FULL_TEXT' in data:     rows['FULL_TEXT'] = str(rows['FULL_TEXT']).replace('\\s+', ' ')
            # *****************************************
            # *****************************************

            if flag.__eq__('INSERT'):
                if 'KEY' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        # '_id': str(rows['KEY']),
                        '_id': str(rows['PRE_FIX']) + '_' + str(rows['KEY']),
                        '_source': dict(rows)
                    }
                elif 'DOCID' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['DOCID']),
                        '_source': dict(rows)
                    }

                elif 'EMPCD' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['EMPCD']),
                        '_source': dict(rows)
                    }
                elif 'ES_EMP_CD' in rows:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_id': str(rows['ES_EMP_CD']),
                        '_source': dict(rows)
                    }
                else:
                    body = {
                        '_index': self.index_name,
                        '_type': self.doc_type,
                        '_source': dict(rows)
                    }

            elif flag.__eq__('DELETE'):
                # print('@@@@@@@@DELETE')
                body = {
                    '_op_type': 'delete',
                    '_index': self.index_name,
                    '_type': self.doc_type,
                    '_id': str(rows['KEY']),
                    # '_id': 'new_id_' + str(loop)
                }

            elif flag.__eq__('UPDATE'):
                body = {
                    '_op_type': 'update',
                    '_index': self.index_name,
                    '_type': self.doc_type,
                    # '_id': str(rows['KEY']),
                    # '_id': str(rows['PRE_FIX']) + '_' + str(rows['KEY']),
                    # '_id': 'new_id_' + str(loop),
                    'doc': dict(rows),
                    'doc_as_upsert': True
                    # 'doc_as_upsert': False
                }

            docs.append(dict(body))

            # print('\n\nbody length ok', '###SEQUENCE >> ', str(loop), self.memory_size, self.get_lists_dict_length(docs))

            if self.get_lists_dict_length(docs) > self.memory_size:
                # log.info(docs)
                log.info('body length_send : ' + str(len(docs)) + ',\t' + str(self.get_lists_dict_length(docs)))
                if processing:
                    log.info('Bulk Processing >> {} -> {} / {}\n'.format(os.getpid(), processing[0], processing[1]))
                self.bulk_send(docs)
                docs.clear()

            # else:
            #     print('==@@@add buffer@@==', self.get_lists_dict_length(docs))

            loop += 1

        # print('\n\nok', ''.join(str(docs).replace(',', '\n')))
        if int(self.get_lists_dict_length(docs) > 0):
            # print('\n')
            log.info('body length_remained_send : ' + str(len(docs)) + ',\t' + str(self.get_lists_dict_length(docs)))
            if processing:
                log.info('Bulk Processing >> {} -> {} / {}\n'.format(os.getpid(), processing[0], processing[1]))
            # print('\n')
            self.bulk_send(docs)
            docs.clear()

        # self.elasticsearch_get_search()


if __name__ == '__main__':
    '''
    lists = [{'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_0', '_source': {'state': 'NY'}}, {'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_1', '_source': {'state': 'NY'}}, {'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_2', '_source': {'state': 'NY'}}, {'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_3', '_source': {'state': 'NY'}}, {'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_4', '_source': {'state': 'NY'}}, {'_type': 'account', '_index': 'bank_version1', '_id': 'new_id_5', '_source': {'state': 'NY'}}]
    print('\n\n', len(lists))

    for token in lists:
        print('ok', token)
        print('\n\n', len(str(token)))
    '''
    indics_type = ['bank_version1', 'account']
    # indics_type = ['posco_law_unfair_trainset', 'es']
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_delete_bu_query()
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_insert_delete_update('INSERT')
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_insert_delete_update('DELETE')
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_insert_delete_update('UPDATE')

    data = [{'sentence': '제1조(시행일) 이 지침은 2017년 1월 1일부터 시행한다.', 'category': '부당특약 심사지침', 'law': '부당특약', 'label': 'pos'}, {'sentence': '제2조(종전 예규의 폐지) 종전 「부당특약 심사지침」은 이를 폐지한다.', 'category': '부당특약 심사지침', 'law': '부당특약', 'label': 'pos'}]
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_delete_bu_query()
    elasticsearch_interface(indics_type[0], indics_type[1]).bulk_add_buffer(data, 'INSERT')

    # delte_data = [{'KEY' : 'piCfEXQBXUqa8NCrfn_u'}, {'KEY' : 'xSBLFXQB2uAYsLae85XL'}, {'KEY' : '8CCeEXQBXUqa8NCre34A'}]
    # elasticsearch_interface(indics_type[0], indics_type[1]).bulk_add_buffer(delte_data, flag='DELETE')

    """
    # 건건이
    delte_data = [[{'KEY': 'xiBLFXQB2uAYsLae85XL'}], [{'KEY': 'i4yjEXQBC6lVwQXf4crp'}], [{'KEY': 'xyChEXQBXUqa8NCrG4BA'}], [{'KEY': '7gDHSXQBUf5zFkybEeh3'}]]
    obj_delete = elasticsearch_interface(indics_type[0], indics_type[1])
    for each_json_list in delte_data:
        # print('each_json_list', each_json_list)
        obj_delete.bulk_add_buffer(each_json_list, flag='DELETE')
    """



