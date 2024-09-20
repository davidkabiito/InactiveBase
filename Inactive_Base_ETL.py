#!/usr/bin/python

"""
Created on      :May 16, 2019
Purpose         :Lock at successful db query execution and upload to DFE, Archive and Application Server for a day.
                 Also send alert mails for success and failure in ETL process.

"""


import os
import sys
import time
import base64
import simplejson
import subprocess
from datetime import datetime, timedelta

main_path = os.path.dirname(os.path.abspath(__file__))
script_name = os.path.basename(__file__).split('.')[0]
corepath = os.path.join(main_path, 'pycore')

if not os.path.exists(corepath):
    print ''
    print 'ERROR :CORE PATH NOT FOUND [%s]' % (corepath)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()
else:
    sys.path.append(corepath)

try:
    from Log_Module.LogUtility import Logger
    from PyUtils_Module.PyUtility import Utils
    from SSH_Module.SSHUtility import SSHAccess
    from DB_Module.OracleDBUtility import Oracleutils
    #from Email_Module.EmailUtility import EmailService
except ImportError, e:
    print ''
    print 'ERROR :Core Modules Import Error :' + str(e)
    print 'Exiting Form Program Execution.'
    print ''
    sys.exit()

### application configuration ###
script_path = os.path.dirname(main_path)
logfilename = script_name + '.log'
log_path = os.path.join(script_path, 'Logs')
Utils.makePath(log_path)
logfile = os.path.join(log_path, logfilename)
log_entry = 'Inactive_Base_ETL'
log = Logger(log_entry, logfile)

config_path = os.path.join(script_path, 'config')
Utils.makePath(config_path)
config_file = os.path.join(config_path, 'config.json')

lock_path = os.path.join(config_path, 'lock')
Utils.makePath(lock_path)
curr_date = datetime.today()
lock_filename = '%s.%s.lock' % (script_name,curr_date.strftime("%d%m%Y"))
lock_file = os.path.join(lock_path, lock_filename)

temp_path = os.path.join(script_path, 'temp')
Utils.makePath(temp_path)

#curr_date = datetime.today()
day_1 = datetime.today() - timedelta(1)

template_path = os.path.join(script_path, 'templates')
Utils.makePath(template_path)
email_template = os.path.join(template_path, 'email_template.html')

def abort_etl():
    log.setLog('INFO', 'Aborting ETL program execution')
    Utils.logtailer(log)

def sendAlert(emailconf, email_data, mail_template):
    sendmail_time = datetime.today().strftime('%d-%b-%Y %H:%M:%S')
    email_header = "Inactive Base Data Uploader Alert Of Date '%s'" % (sendmail_time)
    body ="""
          <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; %s</br>
          <br></br>
          """ % (email_header)

    body += '<h5>' + email_data + '</h5>'

    email_body_html = body
    email_template_html = open(mail_template, 'r').read()
    email = email_template_html % email_body_html
    log.setLog('INFO','MailBody Preparation is Done')

    attachment = False
    log.setLog('INFO', 'Sendmail Process starts')
    email_service = EmailService()
    email_service.secure_sendmail(log, emailconf, email, attachment)

def runshell(cmd):
    cmd_output = ''
    cmd_error = ''

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (sh_output, sh_status) = p.communicate()
    cmd_output = sh_output.strip()
    cmd_error = sh_status.strip().replace('\n', ',')

    return (cmd_output, cmd_error)

def no_upation_db_alert(email_conf, template):
    curr_time = curr_date.strftime('%H%M%S')
    sys_time = curr_date.strftime('%H:%M:%S %p')
    last_chk_time = '153000'

    if int(curr_time) >= int(last_chk_time):
        log.setLog('INFO', 'Final DB Updation Check: Failure-Email-Alert [No data sent for the day]')
        email_data = 'No data for the day: Current Time - %s [No records returned from DB Query execution]' % sys_time
        sendAlert(email_conf, email_data, template)

def removeLock():
    filelist = [ f for f in os.listdir(lock_path) if f.endswith(".lock") ]
    for f in filelist:
        lock_file = os.path.join(lock_path, f)
        os.remove(lock_file)

def makeSuccessLock():
    Utils.makePath(lock_path)
    open(lock_file, 'a').close()

def checkLock():
    exist_stat = os.path.exists(lock_file)
    if exist_stat:
        print 'Script Already Executed Successfully For Today'
        print 'success lock file :%s' % lock_file
        sys.exit()
    else:
        Utils.makePath(lock_path)

def checkScheduletime(start_time, end_time):
    execution_time = datetime.today().strftime('%H%M%S')

    if not (start_time < execution_time < end_time):
        print 'Exiting! [Exceeding scheduled time (Current time :%s)]' % execution_time
        sys.exit()
    else:
        print 'Executing... [Within scheduling time (Current time :%s)]' % execution_time


def main():
    checkLock()
    Utils.logheader(log)
    removeLock()

    log.setLog('INFO', 'Reading Configuration Files')
    configfile_status = Utils.checkFile(log, config_file)
    if configfile_status:
        conf = simplejson.load(open(config_file, 'r'))
    else:
        log.setLog('DEBUG', 'Config File Validation Error [Fix the Config File]')
        Utils.abort(log)

    sh_output = ''
    sh_error = ''
    log.setLog('INFO', 'Initiating Data Extaction Program')

    log.setLog('INFO', 'Invoking DB Config details')
    db_conf = conf["Oracle_db_conf"].copy()
    db_encryption = db_conf["encryption"]
    db_user = db_conf["username"]

    if db_encryption.upper() == 'TRUE':
        db_pswd = base64.decodestring(db_conf["password"])
    else:
        db_pswd = db_conf["password"]

    db_ip = db_conf["dbip"]
    db_port = db_conf["dbport"]
    db_sid = db_conf["dbsid"]

    day_1 = datetime.today() - timedelta(1)
    query_exec_date = day_1.strftime('%Y%m%d')
    db_query_file = os.path.join(config_path, 'Inactive_Base.sql')
    sqlfo = open(db_query_file, 'r')
    sql_qry = [ i.replace('\n', '').strip() for i in sqlfo.readlines() if i.replace('\n', '').strip() ]
    db_query = ' '.join(sql_qry).replace('Execution_Date', query_exec_date)
    db_query_table = 'NEON_DAILY_MSISDN_CHURN'

    db_outputfile = os.path.join(temp_path, 'DB_Output.txt')
    temp_dboutput_file = os.path.join(temp_path, 'DB_Output.tmp')

    log.setLog('INFO', 'DB Query Execution Process Started :%s' % db_query_table)
    print "going to execute db query"

    Oracleutils.executeQuery(log, db_ip, db_user, db_pswd, db_port, db_sid, db_query, db_outputfile)

    sh_cmd = """cp %s %s""" % (db_outputfile,temp_dboutput_file)
    (sh_output, sh_error) = runshell(sh_cmd)
    log.setLog('INFO', 'Validating DB OutPut File')
    validate_db_outputfile = Utils.checkFile(log, db_outputfile)

    if validate_db_outputfile:
        log.setLog('DEBUG', 'DB OutPut File Validation :Success')
    else:
        log.setLog('DEBUG', 'DB OutPut File Validation :Failed :[No records returned from DB Query execution]')
        #no_upation_db_alert(conf, email_template)
        Utils.logtailer(log)

    log.setLog('INFO', 'Initiating Data Upload Program')

    ### Upload to Archive location ###
    log.setLog('INFO', 'Data Upload Program Started for :Neon Archive')
    local_upload_conf = conf["local_upload"].copy()
    archive_path = local_upload_conf["Archive"]

    archive_file_timestamp = datetime.today().strftime('%d%m%Y')
    archive_file = os.path.join(archive_path, 'Inactive_base_%s.csv' % (archive_file_timestamp))
    sh_cmd = """cp %s %s""" % (temp_dboutput_file, archive_file)
    (sh_output, sh_error) = runshell(sh_cmd)
    if sh_error:
        log.setLog('DEBUG', 'Archive data upload command :[%s]' % sh_cmd)
        log.setLog('ERROR', 'Archive data upload :Failed:[Error:%s]' % sh_error)
    else:
        log.setLog('INFO', 'Archive data upload :Success:[File:%s]' % archive_file)

    ### Upload to Neon DFE location ###
    log.setLog('INFO', 'Data Upload Program Started for :Neon DFE')
    dfe_path = local_upload_conf["DFE"]
    temp_dfe_file = os.path.join(dfe_path, 'Inactive_base_%s.csv.tmp' % (archive_file_timestamp))
    dfe_file = os.path.join(dfe_path, 'Inactive_base_%s.csv' % (archive_file_timestamp))

    sh_cmd = """cp %s %s;mv %s %s
             """ % (temp_dboutput_file, temp_dfe_file, temp_dfe_file, dfe_file)
    (sh_output, sh_error) = runshell(sh_cmd)
    if sh_error:
        log.setLog('DEBUG', 'DFE data upload command :[%s]' % sh_cmd)
        log.setLog('ERROR', 'DFE data upload :Failed:[Error:%s]' % sh_error)
        #log.setLog('DEBUG', 'DFE data upload alert :Failure-Email-Alert')
        #email_data = 'DFE data upload: FAILED'
        #sendAlert(conf, email_data, email_template)
        Utils.logtailer(log)
    else:
        makeSuccessLock()
        log.setLog('INFO', 'DFE data upload :Success:[File :%s]' % dfe_file)
        #log.setLog('INFO', 'DFE data upload alert :Success-Email-Alert')
        #email_data = 'DFE data upload: SUCCESS [Uploaded File :%s]' % dfe_file
        #sendAlert(conf, email_data, email_template)

    ### Now sleep for 1 hours 30 minutes [5400 sec] before file uploading to Elita Application Server###
    sleep_time = 3600
    log.setLog('INFO', 'Now execution need to be suspended for seconds :%s' % sleep_time)
    time.sleep(sleep_time)

    ### Upload to Elita Server ###
    remote_upload_conf = conf["remote_upload"].copy()
    remote_upload_jobstatus = remote_upload_conf["start_job"].upper()
    if not remote_upload_jobstatus == "TRUE":
        log.setLog('INFO', 'Remote server upload job status :[%s]'% remote_upload_jobstatus )
        Utils.logtailer(log)

    host_ip = remote_upload_conf["host_ip"]
    user = remote_upload_conf["user"]
    if db_encryption.upper() == 'TRUE':
        pswd = base64.decodestring(remote_upload_conf["password"])
    else:
        pswd = remote_upload_conf["password"]

    upload_loc = remote_upload_conf["upload_loc"]

    elita_upload_timestamp = datetime.today().strftime('%d%m%Y%H%M%S')
    upload_file = os.path.join(upload_loc, '%s' % (elita_upload_timestamp))
    Elita_uploaded_file_copy = os.path.join(temp_path, '%s.csv' % elita_upload_timestamp)
    Day_minus_one = datetime.today() - timedelta(1)
    Day_minus_one = Day_minus_one.strftime("%Y-%m-%d")
    Day_minus_one = Day_minus_one+" 00:00:00"
    log.setLog('INFO', 'Data Upload Program Started for :Elita server [%s]' % host_ip)

    sh_cmd = """cat %s|awk 'BEGIN {FS ="," ;OFS=","} {if((($9=="HIGH ARPU" && (($6>5 && $6<15)||($6>29 && $6<250)|| ($4>5 && $4<15))) || ($10=="YES" && (($6>30 && $6<90
)|| ($4>6 && $4<90)))|| ($9!="HIGH ARPU" && $10!="YES" && (($4>6 && $4<30)||($6>5 && $6<15)||($6>30 && $6<120)))) && $2!="Alo _winback_VL" && $2!="ALO_winback" && $2!="
MBB_Wide" && $2!="MBB_Corporate" && $2!="TwinSIM" && $2!="PrepaidPro" && $2!="Pre_activation_vl" && $2!="Pre_activation_mid" && $2!="Pre_activation_High" && $2!="Pre_ac
tivation_LOW" && $2!="Registration-RGS90" && $2!="Pre_activation" && $13=="N" ){if ($12=="%s"){print $1}}}'>%s""" % (archive_file,Day_minus_one,Elita_uploaded_file_copy
)
    print upload_file
    log.setLog('INFO', 'Executing the command  :[%s]' % sh_cmd)
    log.setLog('INFO', 'Serching  the recodes with Inactive base count > 5 and Date pattern [%s]' % Day_minus_one)
    (sh_output, sh_error) = runshell(sh_cmd)
    log.setLog('INFO', 'Command Execution status:[%s]'% sh_error)
    log.setLog('INFO', 'Elita server upload local copy is available at:[%s]'% Elita_uploaded_file_copy)
    print  Elita_uploaded_file_copy
    validate_Elita_uploadfile = Utils.checkFile(log, Elita_uploaded_file_copy)
    if not validate_Elita_uploadfile:
        log.setLog('ERROR', 'File is empty :[Error :%s]' % sh_error)
        log.setLog('INFO', 'Cancel Application Server file uploading')
    else:
        scp_status = False
        remote_serv_con = SSHAccess(log, host_ip, user, pswd)
        scp_status = remote_serv_con.scp_put(Elita_uploaded_file_copy, upload_file)
        if scp_status:
            log.setLog('INFO', 'Elita data upload status :Success: [File:%s]' % (upload_file))
            #log.setLog('INFO', 'Elita data upload alert: Success-Email-Alert')
            #email_data = 'Application Server[%s] Data upload: SUCCESS [Uploaded File :%s]' % (host_ip, upload_file)
            #sendAlert(conf, email_data, email_template)
        else:
            log.setLog('ERROR', 'Elita data upload status :Failed:[Error :%s]' % scp_status)
            log.setLog('INFO', 'Elita data upload alert : Failure-Email-Alert')
            #email_data = 'Application Server[%s] Data upload: FAILED' % host_ip
            #sendAlert(conf, email_data, email_template)

    Utils.logtailer(log)


if __name__ == '__main__':
     main()

#END
