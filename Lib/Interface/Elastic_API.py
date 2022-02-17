import os
import json, requests
import ES_Bulk_Incre_Project.Lib.Interface.Elastic_Bulk as Bulk
import ES_Bulk_Incre_Project.Lib.Logging.Logging as log

API_REQEUST = {
    "_cat/indices" : "_cat/indices?format=json&pretty",
    "_cat/alias" : "_cat/aliases?format=json&pretty"
}


class elasticserch_utils:

    def __init__(self):
        self.PATH = '/ES/ES_Bulk_Incre_Project/Output/'
        self.FILLE = 'aliases_json_format'
        self.INDICES = 'indices_json_format'

        self.aliase_dict = {}
        self.aliase_query = []


    def pp_json(self, json_thing, sort=True, indents=4):
        """

        :param json_thing:
        :param sort:
        :param indents:
        :return:
        """

        results = ''
        if type(json_thing) is str:
            results = json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents)
        else:
            results = json.dumps(json_thing, sort_keys=sort, indent=indents)

        print(results)

        return results


    def set_make_alias(self, indices_name_list, alias_name):
        """
        # new_alias = '{"actions": [{"add": {"indices": ["test_idx","posco_ecm_grp2_idx"],"alias": "TEST"}}]}'
        :return:
        """

        try:
            # aliases_add = '{"actions": [{"add": {"indices": [' + indices_name_list + '],"alias": ' + alias_name + '}}]}'
            aliases_add = '{"add": {"indices": [' + indices_name_list + '],"alias": ' + alias_name + '}}'
            print('\n\n')
            print('#' * 40)
            print('## FORMAT')
            print(aliases_add)
            print('#' * 40)
            # print(json.dumps(aliases_add, indent=2, ensure_ascii=False))

            # results = self.pp_json(aliases_add)
            self.aliase_query.append(aliases_add)
            # self.write_file_string(results, self.FILLE)

        except Exception as ex:  # 에러 종류
            # start_time_iterable = datetime.datetime.now()
            print('Error >> ', ex)
            pass

        # print('euiyoyng', obj.aliase_query)

        # return self.aliase_query


    # noinspection PyUnusedLocal,PyShadowingBuiltins
    def write_file_string(self, results, file_name):
        write_file_name = file_name

        open_output_file = open(self.PATH + write_file_name, 'a', -1, "utf-8")
        open_output_file.write(str(results) + '\n')
        open_output_file.close()



class elasticsearch_httpconnection:
    """
    https://programtalk.com/python-examples/elasticsearch.helpers.bulk/
    """

    def __init__(self):
        self.HEADER = {'Content-Type': 'application/json', 'Authorization': 'Basic TEST==', 'Connection': 'close'}
        self.SERVER_IP = "x.x.x.x:9200"
        self.TARGET_IP = "x.x.x.x:9201"
        self.REQUEST_JSON = API_REQEUST


    def get_source_target_server_list(self):
        return [self.SERVER_IP, self.TARGET_IP]


    def get_cat_indices(self, SERVER_IP):
        """

        :param SERVER_IP:
        :return:
        """
        url = 'http://' + SERVER_IP + '/' + self.REQUEST_JSON["_cat/indices"]
        print('url', url)
        # print(self.REQUEST_JSON["_cat/alias"])
        # url = 'http://x.x.x.x:9210/_cat/aliases?format=json&pretty'
        try:
            doc = requests.get(url=url, headers=self.HEADER, timeout=60)
            print(json.dumps(doc.json(), indent=2, ensure_ascii=False))

        except Exception as ex:  # 에러 종류
            # start_time_iterable = datetime.datetime.now()
            print('Error >> ', ex)
            pass

        return doc


    def make_indices_json_format(self, source_dic, target_dic):
        """
        source_dic {indices_name, count}
        target_dic {indices_name, count}

        ex. "contents-feeder-server-log-2019-09-03": "673",
        :param doc_json:
        :return:
        """
        print('\n\n')
        print('make_indices_json_format')

        # print(doc_json)
        merge_indices_dict = {}
        for json_row in source_dic:
            # print('loop', json_row['index'], json_row['docs.count'])
            merge_indices_dict[json_row['index']] = json_row['docs.count']

        for json_row in target_dic:
            if json_row['index'] in merge_indices_dict.keys():
                merge_indices_dict[json_row['index']] = merge_indices_dict[json_row['index']] + "\t" + json_row['docs.count']
            else:
                merge_indices_dict[json_row['index']] = json_row['docs.count']

        # print('merge_indices_dict ', json.dumps(merge_indices_dict, indent=2, ensure_ascii=False))

        obj = elasticserch_utils()

        # json alias query file remove
        if os.path.exists(obj.PATH + obj.INDICES):
            os.remove(obj.PATH + obj.INDICES)

        # file write
        for key, values in merge_indices_dict.items():
            obj.write_file_string(str(key) + '\t' + str(values), obj.INDICES)

        print('\nMake File for indices Finished...')


    def get_cat_alias(self):
        """
        url http://10.132.57.74:9210/_cat/alias?format=json&pretty
        :return:
        """
        url = 'http://' + self.SERVER_IP + '/' + self.REQUEST_JSON["_cat/alias"]
        print('url', url)
        # print(self.REQUEST_JSON["_cat/alias"])
        # url = 'http://x.x.x.x:9210/_cat/aliases?format=json&pretty'
        print('url', url)
        try:
            doc = requests.get(url=url, headers=self.HEADER, timeout=60)
            # print(json.dumps(doc.json(), indent=2, ensure_ascii=False))

        except Exception as ex:  # 에러 종류
            # start_time_iterable = datetime.datetime.now()
            print('Error >> ', ex)
            pass

        return doc


    def set_make_alias_send(self, indices_name_list, alias_name):
        """
        # new_alias = '{"actions": [{"add": {"indices": ["test_idx","test2_idx"],"alias": "TEST"}}]}'
        :return:
        """
        # url = 'http://' + self.SERVER_IP + '/' + '_aliases'
        url = 'http://' + self.TARGET_IP + '/_aliases'
        try:
            aliases_add = '{"actions": [{"add": {"indices": [' + indices_name_list + '],"alias": ' + alias_name + '}}]}'
            print('\n\n')
            print('#' * 40)
            print('## REQ')
            print(aliases_add)
            print('#' * 40)
            doc = requests.post(url=url, headers=self.HEADER, data=aliases_add, timeout=60)
            # print('\n')
            print(json.dumps(doc.json(), indent=2, ensure_ascii=False))

        except Exception as ex:  # 에러 종류
            # start_time_iterable = datetime.datetime.now()
            print('Error >> ', ex)
            pass


    def set_make_alias_test(self):
        """
        # new_alias = '{"actions": [{"add": {"indices": ["test_idx","test2_idx"],"alias": "TEST"}}]}'
        :return:
        """
        # url = 'http://' + self.SERVER_IP + '/' + '_aliases'
        url = 'http://' + self.TARGET_IP + '/_aliases'
        try:
            indices_name_list = '"test1_idx","test2_idx"'
            alias_name = '"TEST"'
            aliases_add = '{"actions": [{"add": {"indices": [' + indices_name_list + '],"alias": ' + alias_name + '}}]}'
            doc = requests.post(url=url, headers=self.HEADER, data=aliases_add, timeout=60)
            print('\n')
            print(json.dumps(doc.json(), indent=2, ensure_ascii=False))

        except Exception as ex:  # 에러 종류
            # start_time_iterable = datetime.datetime.now()
            print('Error >> ', ex)
            pass


    def make_alias_json_format(self, doc_json):
        """

        :param doc_json:
        :return:
        """
        print('\n\n')
        print('make_alias_json_format')
        # print(doc_json)
        dict = {}

        is_enable_migration_new_engine = True
        # is_enable_migration_new_engine = False

        is_enable_merge_alias_make = True
        # is_enable_merge_alias_make = False

        # Each Indics to Alias (SEND)
        if is_enable_migration_new_engine:
            for json_row in doc_json:
                # print('loop', json_row['index'], json_row['alias'])
                # POST Make Alias
                elasticsearch_httpconnection().set_make_alias_send('"' + json_row['index'] + '"', '"' + json_row['alias'] + '"')


        if is_enable_merge_alias_make:
            # indics 가 신규검색엔진에  다 있어야함
            for json_row in doc_json:
                # print('loop', json_row['index'], json_row['alias'])
                if not str(json_row['alias']) in dict.keys():
                    dict[str(json_row['alias'])] = '"' + str(json_row['index']) + '"' + ","
                else:
                    dict[json_row['alias']] = str(dict[json_row['alias']]) + '"' + str(json_row['index']) + '"' + ","

            # print('dict', dict)
            dict_doc_outupt = {}

            obj = elasticserch_utils()
            # json alias query file remove
            if os.path.exists(obj.PATH + obj.FILLE):
                os.remove(obj.PATH + obj.FILLE)

            obj.aliase_query.append('{"actions": [')
            for key, values in dict.items():
                # print('loop', key, values)
                dict_doc_outupt[key] = str(values)[:len(str(values))-1]
                print('loop', key, dict_doc_outupt[key])

                # POST Make Alias
                # elasticsearch_httpconnection().set_make_alias_send(dict_doc_outupt[key], '"' + key + '"')
                obj.set_make_alias(dict_doc_outupt[key], '"' + key + '"')

            print('\n\n')
            # elasticserch_utils().aliase_dict["actions"] = query
            obj.aliase_query.append(']}')
            whole_query = str(','.join(obj.aliase_query)).replace('[,', '[').replace(',]', ']')
            # print(whole_query)
            print(obj.pp_json(whole_query))
            obj.write_file_string(obj.pp_json(whole_query), obj.FILLE)
            # print('dict_doc_outupt', dict_doc_outupt)

        return dict_doc_outupt



if __name__ == '__main__':

    is_enable_get_alias_to_set = True
    # is_enable_get_alias_to_set = False

    obj_gather_indics = elasticsearch_httpconnection()
    print('\n')
    print('obj_gather_indics.get_source_target_server_list() ', obj_gather_indics.get_source_target_server_list())

    source_dic, target_dic = {}, {}
    server_list = obj_gather_indics.get_source_target_server_list()
    source_dic = obj_gather_indics.get_cat_indices(server_list[0]).json()
    target_dic = obj_gather_indics.get_cat_indices(server_list[1]).json()
    # print('\n', source_dic, target_dic)
    obj_gather_indics.make_indices_json_format(source_dic, target_dic)


    if is_enable_get_alias_to_set:
        # elastic_alias_migration
        print('\n\n')
        print('## OUTPUT (get_cat_aliase) ##')
        doc_json = elasticsearch_httpconnection().get_cat_alias()

        # print('\n\n')
        # print('## OUTPUT (set_make_alias_test) ##')
        # elasticsearch_httpconnection().set_make_alias_test()

        print('\n\n')
        print('## OUTPUT (in memory) ##')
        # print('@doc_json@', doc_json.json())

        # POST alias to TARGET
        elasticsearch_httpconnection().make_alias_json_format(doc_json.json())


