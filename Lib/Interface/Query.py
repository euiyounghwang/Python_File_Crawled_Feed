
def elastic_query_match_all_search():
    """
    body = {
                'query': {
                'match': {
                        'STATE': 'NY'
                    }
                }
           }
    :return:
    """
    query_string = {
        "query": {
            "match_all": {}
        }

    }

    return query_string