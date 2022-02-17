import json

import jaydebeapi as jdb_api
import pandas as pd
import pandas.io.sql as pd_sql
from pandas import DataFrame

import ES_Bulk_Incre_Project.Config.getConfig as Config
import ES_Bulk_Incre_Project.Lib.Logging.Logging as log

# noinspection PyShadowingNames
import pympler.asizeof
import os
import jpype
import inspect



# noinspection PyMethodMayBeStatic,PyShadowingNames
class DB_Transaction_Cls:

    def __init__(self, model_classfy, environment):
        '''
        jar='/ES/ES_UnFair_Detection/lib/Reference_Library/ojdbc6.jar'
        args = '-Djava.class.path=%s' % jar

        jvm_path = jpype.getDefaultJVMPath()
        jpype.startJVM(jvm_path, args)
        '''
        self.model_classfy = model_classfy
        self.env = environment

        # sys.path.append("/ES/")

    def get_connection(self):
        return self.conn

    # noinspection PyAttributeOutsideInit
    def set_connection(self):

        if self.env:
            log.info('[' + inspect.currentframe().f_code.co_name + '] set_connection >> PROD Environment')
            self.conn = jdb_api.connect('oracle.jdbc.driver.OracleDriver',
                                        'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=x.x.x.x)(PORT=x))(ADDRESS=(PROTOCOL=TCP)(HOST=x.x.x.x)(PORT=x))(FAILOVER=on)(LOAD_BALANCE=on))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=x)))',
                                        ['x',
                                         'x'],
                                        Config.File().Server_JDBC_Driver)
        else:
            log.info('[' + inspect.currentframe().f_code.co_name + '] set_connection >> DEV Environment')

            self.conn = jdb_api.connect('oracle.jdbc.driver.OracleDriver',
                                        'jdbc:oracle:thin:@x.x.x.x:x:x',
                                        ['x',
                                         'x'],
                                        Config.File().Server_JDBC_Driver)

        return self.conn

    def set_disconnection(self, conn):
        """

        :param conn:
        :return:
        """
        if conn:
            if not conn._closed:
                conn.close()
                # print('set_disconnection() >> DB Connection Closed...')


if __name__ == '__main__':
    system_id = 'x'
    conn = DB_Transaction_Cls(system_id).set_connection()
    DB_Transaction_Cls(system_id).set_disconnection(conn)



