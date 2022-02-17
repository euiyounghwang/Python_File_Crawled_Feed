import os
import datetime
import configparser
import sys
import yaml
import json

# sys.path.append('/TOM/ES/')
# sys.path.append('/ES/')

import ES_Bulk_Incre_Project.Config.getConfig as Config

config = configparser.ConfigParser()
config.read(Config.File().get_Root_Directory() + '/ES_Bulk_Incre_Project/Lib/DB/DB_Conf.ini')


class db_config_setting:

    def get_bulk_sql_select_sql(self, system_classfy):
        """

        :param system_classfy:
        :return:
        """
        return config.get('FEED_BULK_ECM_DELETE_VIEW', 'query')
