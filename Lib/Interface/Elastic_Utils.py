
import json
import ES_Bulk_Incre_Project.Lib.Logging.Logging as log

def elasticsearch_response(results_list):
    """
    response elasticsearch 7.X version
    :param results_list:
    :return:
    """
    print('\n\nelasticsearch_response function called..')
    # print(results)
    response_ack = []
    feed_sum_indexing_count , feed_success_total_count, feed_fail_total_count = 0, 0, 0

    if results_list:
        # print('\nresults', results_list)
        for rows in results_list:
            # print('row', rows)
            each_row = json.loads(rows)

            if 'index' in each_row:
                if str(each_row['index']['status']).__contains__('2'):
                    response_ack.append('[CS] ' + each_row['index']['_id'])
                    feed_success_total_count += 1
                else:
                    # log.error('curl_file_command -> index' + ' >> ' + results)
                    response_ack.append('[CF] ' + each_row['index']['_id'])
                    feed_fail_total_count += 1

            elif 'update' in each_row:
                if str(each_row['update']['status']).__contains__('2'):
                    response_ack.append('[US] ' + each_row['update']['_id'])
                    feed_success_total_count += 1
                else:
                    # log.error('curl_file_command -> index' + ' >> ' + results)
                    print('@@@', each_row[1])
                    response_ack.append('[UF] ' + each_row['update']['_id'])
                    feed_fail_total_count += 1

            elif 'delete' in each_row:
                if str(each_row['delete']['status']).__contains__('2'):
                    response_ack.append('[DS] ' + each_row['delete']['_id'])
                    feed_success_total_count += 1
                else:
                    # log.error('curl_file_command -> index' + ' >> ' + results)
                    print('@@@', each_row[1])
                    response_ack.append('[DF] ' + each_row['index']['_id'])
                    feed_fail_total_count += 1

                feed_sum_indexing_count += 1

    print('\nresults >> ', ' '.join(response_ack))

    return feed_success_total_count, feed_fail_total_count, feed_sum_indexing_count


if __name__ == '__main__':
    response_list = []
    response_list.append(json.dumps({'index': {'_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_index': 'bank_version1', 'forced_refresh': True, '_seq_no': 49559, '_id': 'CU_pAHQB0wDKL2WMW8HX', '_version': 1, '_type': '_doc', 'result': 'created', 'status': 201, '_primary_term': 1}}))
    response_list.append(json.dumps({'index': {'_shards': {'total': 2, 'successful': 2, 'failed': 0}, '_index': 'bank_version1', 'forced_refresh': True, '_seq_no': 49559, '_id': 'CU_pAHQB0wDKL2WMW8HX', '_version': 1, '_type': '_doc', 'result': 'created', 'status': 201, '_primary_term': 1}}))
    # data = json.loads(response)
    # data = json.dumps(response)
    print('response_list ', response_list)
    print(elasticsearch_response(response_list))
