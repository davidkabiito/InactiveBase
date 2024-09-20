#!/usr/bin/python

"""
Created on            :Sep 06, 2013
Author                :Rudra Prasad
Modified By           :
Date Of Modification  :
Purpose               :Python Script Oracle DB utilities
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

import pyodbc

################### adding log module ###################

script_path = os.path.dirname(os.path.abspath(__file__))
corelog_path = os.path.join(script_path, '../Log_Module')

if not os.path.exists(corelog_path):
    print ''
    print 'ERROR :From pycore.DB_Module :CORE LOG PATH NOT FOUND [%s]' % (corelog_path)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()
else:
    sys.path.append(corelog_path)

try:
    from LogUtility import Logger
except ImportError, e:
    print ''
    print 'ERROR :From pycore.LogModule :Core Modules Import Error :' + str(e)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()

############################################################

class Mssqlutils(object):
    @classmethod
    def get_error(cls, errorlog=None):
        if errorlog:
            print ''
            print errorlog
            print 'Exiting Form Program Execution.'
            print ''
            sys.exit()

    @staticmethod
    def makePath(dirpath):
        error = None
        if not os.path.exists(dirpath):
            sh_cmd = '''mkdir -p "%s" ;echo $?''' % (dirpath)
            exit_status = int(os.popen(sh_cmd).read().strip())
            if exit_status != 0:
                dir_loc = os.path.dirname(dirpath)
                if not os.path.exists(dir_loc):
                    error = 'ERROR :Path is incrorrect :%s' % (dirpath)
                else:
                    error = 'ERROR :Unable to Create Directory :Permission Issue :%s' % (dirpath)

                Mssqlutils.get_error(error)

    @staticmethod
    def abort(log):
        log.setLog('DEBUG', 'Terminating Program execution')
        log.setLog('INFO','')
        log.setLog('INFO', 'Program Execution Ended')
        log.setLog('INFO','----------------------------------------------------')
        log.setLog('INFO','')
        sys.exit()

    @staticmethod
    def checkPath(dirpath):
        exist_status = False
        if os.path.exists(dirpath):
            exist_status = True
        else:
            log.setLog('ERROR','Path does not exist [%s]' % (dirpath))

        return exist_status

    @staticmethod
    def checkFile(log, chkfile):
        success = False
        if os.path.exists(chkfile):
            if os.path.isfile(chkfile):
                if  os.stat(chkfile)[6]==0:
                    log.setLog('DEBUG','File is Empty [%s]' % (chkfile))
                else:
                    success = True
            else:
                if os.path.isdir(chkfile):
                    log.setLog('ERROR', 'This is a directory [%s]' % (chkfile))
                else:
                    log.setLog('ERROR', 'Error in this input path [%s]' % (chkfile))
        else:
            log.setLog('DEBUG','File Does Not Exist [%s]' % (chkfile))

        return success

    @staticmethod
    def executeQuery(log, db_ip, db_user, db_pass, db_sid, db_driver, db_query, db_output_file):
        dbout = open(db_output_file, 'w')
        try :
            log.setLog('INFO', 'Trying to Connect DB[%s]...' % (db_ip))
            db = pyodbc.connect('DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (db_driver, db_ip, db_sid, db_user, db_pass))
            log.setLog('INFO', 'DB[%s] Connection :Success' % (db_ip))
        except Exception, conn_exp:
            log.setLog('ERROR', 'Error in DB[%s] Connection :%s' % (db_ip, str(conn_exp).replace('\n', ' ')))
            log.setLog('DEBUG', 'Exiting From Program Execution [Check DB details in Config File]')
            Mssqlutils.abort(log)

        try:
            cursor = db.cursor()
            cursor.execute(db_query)
            for result in cursor:
                line = ""
                for val in result:
                    if line == "":
                        line = str(val)
                    else:
                        line = line + "," + str(val)

                dbout.write(str(line) + ', \n')

            log.setLog('INFO', 'DB Query Execution Completed :Success')
            cursor.close()
            db.close()
            dbout.close()
        except Exception, exec_e:
            log.setLog('ERROR', 'Error in DB query Execution :%s' % str(exec_e))

#END
