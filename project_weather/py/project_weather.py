#!/usr/bin/python

from __future__ import print_function
import ConfigParser
import datetime
import logging
import os
import collections
import subprocess
import sys
import time
import traceback
ORIGINAL_DIR = os.getcwd()


# ===============================================================
# Python script wrapper to run  hive  to prepare weather data for analysis.
# This script performs the following:
#    1. Retrieves 6 mandatory parameters from command line
#           python_script:      Python script name
#           work_dir:           Changes directory to location of code
#           batch_cfg:          Configuration file that contains all the parameters and log file information
#           job_cfg:            Configuration file that contains all the parameters need for specific job like script,
#                               source/target directories etc
#           restart_script_no:  Restart script number. If there are more than one script/table is called from this job, in
#                               the event of failure, it will be used to restart
#           restart_step_no:    Restart step number. If there are more than one script/step is called from this job, in
#                               the event of failure, it will be used to restart
#
#           local testing -->   python /rhome/userid/project_weather/py/project_weather.py ./config/qa/batch.cfg ./config/qa/job-name.cfg 0 0
#
#
#       starts logging job information
#
#   2. Get Batch configuration information
#           Following information is obtained from the the batch cfg file:
#               select_start = start timestamp for monthly ingestion
#               select_end = end timestamp for monthly ingestion
#               ingestion_date = date of ingestion if applicable for the process
#               ingestion_key = ingestion key if applicable for the process
#               ingestion_seq = ingestion sequence if applicable for the process
#               job_queue =  job queue name - default
#               ts_log = timestamp log path
#               job_log = job log path
#
#   3. Get Job configuration information
#           Following information is obtained from the the job cfg file:
#
#               hd_log_dir = directory path for HDFS logs
#               hive_log_dir = hive log path
#               hive_script_dir = directory path to the hive scripts
#               file_name = location where the data file resides
#               hdfs_dir = directory path to HDFS
#               total_scripts = total number of scripts called from the python script
#               hive_ext = hive file extension (.hive or .txt)
#               db_etl = Source database
#               db_tgt = Target database

#       If scripts run is successful, exit 0
#
#
# RETURN CODES:
#      0: Success
#      1: Problem with passed in command line arguments
#      2: Problem in setting Select_start, select_end and ingestionkey in batch configuration file
#      3: Problem reading or getting batch configuration file
#      4: Problem reading or getting job configuration file
#      5: hive load script failed
# ===============================================================

# ===============================================================
#  FUNCTION: get_job_params
#   Opens job config  file to retrieve global params
#
#    params
#     job_cfg: indicates which file to open.
#     exit_code: exit_code for the function
#
#    return: parameter values set for the job
# ===============================================================
def get_job_params(job_cfg, exit_code):
    try:
        job_config = ConfigParser.RawConfigParser()
        job_config.read(job_cfg)

        params = {
            'hd_log_dir': job_config.get('DIRS', 'hd_log_dir'),
			'file_name': job_config.get('DIRS', 'file_name'),
            'hive_script_dir': job_config.get('DIRS', 'hive_script_dir'),
            'db_tgt': job_config.get('DB', 'db_tgt'),
            'hive_ext': job_config.get('SCRIPT_EXT', 'hive_ext'),
            'hive_table_items': job_config.items("HIVE_TABLES_LIST")
            
        }

        return params
    except SystemExit as e:
        job_cfg_err_msg = 'Job Configuration file was not read successfully: ' + traceback.format_exc(900)
        logging.error(job_cfg_err_msg)
        sys.exit(exit_code)
        
 # ===============================================================
 #  FUNCTION: get_ts
 #   Opens config file to retrieve start and end time stamp
 #
 #    params
 #     cfgFile: indicates which file to open.
 #
 #    return: start and end time stamp
 # ===============================================================

def get_ts(batch_cfg):
    try:
        config = ConfigParser.RawConfigParser()
        config.read(batch_cfg)

        params = {
            'select_start': config.get('TS', 'select_start'),
            'select_end': config.get('TS', 'select_end')
        }
        strt = params['select_start']
        end = params['select_end']
        rtr = [strt, end]
        return rtr
    except SystemExit as e:
        err_msg = 'File was not read successfully: ' + traceback.format_exc(900)
        logging.error(err_msg)
        sys.exit(exit_code)

# ===============================================================
#  FUNCTION: set_ts
#   Opens config file.  sets the start date and end date
#   sets the ingestion key
#
#
#    return:
# ===============================================================


def set_ts():
    try:

        config = ConfigParser.RawConfigParser()
        config.read(batch_cfg)


        params2 = {
            'select_start': config.get('TS', 'select_start'),
            'select_end': config.get('TS', 'select_end'),
            'ingestion_key': config.get('INGESTION_DATA', 'ingestion_key'),
            'ingestion_seq': config.get('INGESTION_DATA', 'ingestion_seq'),
            'ingestion_date': config.get('INGESTION_DATA', 'ingestion_date')
        }
        slt_end = params2['select_end']

        set_start = slt_end
        set_end = datetime.datetime.now()
        set_key = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        config.set('TS', 'select_start', str(set_start))
        config.set('TS', 'select_end', str(set_end))
        config.set('INGESTION_DATA', 'ingestion_key', str(set_key))
        config.set('INGESTION_DATA', 'ingestion_date', str(datetime.date.today()))


        with open(batch_cfg, 'wb') as configfile:
            config.write(configfile)

        ts = get_ts(batch_cfg)
        logging.info('Timestamp was successfully set to: start_ts= ' + ts[0] + ' and end_ts= ' + ts[1])

    except SystemExit as e:
        ts_err_msg = 'File was not written successfully: ' + traceback.format_exc(900)
        logging.error(ts_err_msg)
        sys.exit(exit_code)


# ===============================================================
#  FUNCTION: get_batch_parms
#   Opens batch config  file to retrieve global parms
#
#    params
#     batch_cfg: indicates which file to open.
#
#    return: hadoop log location and yarn queue
# ===============================================================

def get_batch_parms(batch_cfg, exit_code):
    try:
        config = ConfigParser.RawConfigParser()
        config.read(batch_cfg)

        params = {
            'job_log': config.get('LOG_DIRS', 'job_log'),
            'ts_log': config.get('LOG_DIRS', 'ts_log'),
            'job_queue': config.get('QUEUE', 'job_queue'),
            'ingestion_date': config.get('INGESTION_DATA', 'ingestion_date'),
            'ingestion_key': config.get('INGESTION_DATA', 'ingestion_key'),
            'select_start': config.get('TS', 'select_start'),
            'select_end': config.get('TS', 'select_end')
        }

        return params
    except SystemExit as e:
        err_msg = 'File was not read successfully: ' + traceback.format_exc(900)
        logging.error(err_msg)
        sys.exit(exit_code)




# ===============================================================
#  FUNCTION: load_log
#   If job fails this function will load the log file so it is easily accessible
#
#    params
#       exit_code : exit code value from the step its being called.
#
#    return: 0 on success
# ===============================================================
def load_log(exit_code):
    try:

        cmd = ['hdfs', 'dfs', '-put', log_file, hd_log_dir]
        log_ret_code = call_cmd(cmd, False)

        if log_ret_code != 0:
            log_err_msg = 'Log File was not written to HDFS successfully: '
            logging.error(log_err_msg)
            sys.exit(exit_code)

    except SystemExit as e:
        log_err_msg = 'File was not written to hadoop successfully: ' + traceback.format_exc(900)
        logging.error(log_err_msg)
        sys.exit(exit_code)

    return log_ret_code


# ===============================================================
#  FUNCTION: call_cmd
#    Executes a shell command.  Logs command, elapsed time to
#    execute command, and error code if command does not return
#    0.
#
#    params
#      cmd: array containing command to execute
#      shl: indicates whether the command should be a one line shell command (If yes then true, else false)
#
#
#    return: 0 on success
# ===============================================================
def call_cmd(cmd, shl):
    logging.info('> ' + ' '.join(map(str, cmd)))
    start_time = time.time()
    command_process = subprocess.Popen( cmd,shell=shl,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)
    command_output = command_process.communicate()[0]
    logging.info(command_output)
    elapsed_time = time.time() - start_time
    logging.info(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)) + ' = time to complete (hh:mm:ss)')
    if command_process.returncode != 0:
        logging.error('ERROR ON COMMAND: ' + ' '.join(map(str, cmd)))
        logging.error('ERROR CODE: ' + str(ret_code))
    return command_process.returncode


#===============================================================
#  FUNCTION: queryWithDBParms
#    Excecutes a file containing a HQL that takes <db_tgt> and <hive_export_dir> params
#
#    params
#      db_tgt: hive var to pass to hive_script
#      hive_export_dir: hive var containing location to store HQL result
#      hive_script: file containing HQL to run
#
#    return: 0 on success
#
#    BEELINE command
#      > beeline --hiveconf mapreduce.job.queuename=<queue> --hivevar db_tgt=<db_tgt>  -f <hive_script>;'
#===============================================================
def queryWithDBParms(job_queue,db_tgt,file_name,select_start,select_end,ingestion_key,hive_script):
    ret_code = 1
    cmd=['beeline','--hiveconf', 'tez.queue.name='+job_queue, '--hivevar',  'db_tgt='+db_tgt, '--hivevar',  'file_name='+file_name, '--hivevar', 'select_start='+select_start,  '--hivevar', 'select_end='+select_end, '--hivevar', 'ingestion_key='+ingestion_key, '-f', hive_script]
    ret_code = call_cmd(cmd,False)
    if ret_code != 0:
        err_message1 = 'Job did not complete successfully when executing ' + hive_script
        err_message2 = '. Fix the problem with hive script and restart with value of ' + str(current_step)
        err_message3 = " in parameter 5 (restart_table_no) and " + str(restart_step)  + " in parameter 6 (restart_step_no) in the command."
        logging.error(err_message1 + err_message2 + err_message3)
        load_log(exit_code)
        sys.exit(exit_code)
    return ret_code


# Start of script
try:
# Get config file ,job file and start position from command line, (list in Python, which contains the command-line arguments passed to the script) must pass through command line or it will fail
    args = sys.argv
    num_of_args =6
    exit_code = 1
    param_msg = " /rhome/userid/project_weather/py/project_weather.py /rhome/userid/project_weather ./config/qa/batch.cfg ./config/qa/job-name.cfg 0 0"


    if int(num_of_args) != len(args):
        print("Invalid number of argumnents: " + str(sys.argv) + " : received : " + str(len(args)) + " and expected: " +
              str(num_of_args) +  " arguments including the script as first args")
        print('Expected arguments should appear in order defined here:')
        print("prod config in autosys --> " + param_msg )
        print("local testing --> /rhome/userid/project_weather/py/project_weather.py " + param_msg)
        print("variable_details --> python_script work_dir batch_cfg job_cfg ")
        sys.stderr.write("Invalid number of argumnents: " + str(sys.argv) + " : received : " + str(len(args)) + " and expected: " +
              str(num_of_args) +  " arguments including the script as first args")
        sys.stderr.write('Expected arguments should appear in order defined here:')
        sys.stderr.write("prod config in autosys --> " + param_msg )
        sys.stderr.write("local testing --> /rhome/userid/project_weather/py/project_weather.py " + param_msg)
        sys.stderr.write("variable_details --> python_script work_dir batch_cfg job_cfg ")
        sys.exit(exit_code)
    else:
        print_string = ''
        for value in args:
            print_string = print_string + value + " "
        print(print_string)

    wrk_dir = args[1]
    batch_cfg = args[2]
    job_cfg = args[3]
    restart_table_no = args[4]
    restart_step_no = args[5]
    # changes directory to code location
    os.chdir(wrk_dir)

    # set up logging for TimeStamp Log
    batch_params = get_batch_parms(batch_cfg, exit_code)
    log_ext= time.strftime('_%Y%m%d_%H%M%S.log')
    ts_log_dir = batch_params['ts_log']
    ts_log_file = ts_log_dir+'ts'+log_ext

    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',filename=ts_log_file,filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    if int (restart_step_no)== 0:
       logging.info("================== START SETTING TIMESTAMP  =============================")
       logging.info("> " + " ".join(map(str, sys.argv)))
       logging.info("log file: " + ts_log_file)
       set_ts();
       logging.info("================== SETTING TIMESTAMP SUCESSFUL =============================")

    loggers = logging.getLogger('')
    handlers = list(loggers.handlers)
    for handler in handlers:
       loggers.removeHandler(handler)
       handler.flush()
       handler.close()

    # set up batch config file and get values
    exit_code = 3
    batch_params = get_batch_parms(batch_cfg, exit_code)

    # define global variables for batch parameters
    job_log_dir = batch_params['job_log']
    job_queue = batch_params['job_queue']
    ingestion_date = batch_params['ingestion_date']
    ingestion_key = batch_params['ingestion_key']
    select_start = batch_params['select_start']
    select_end = batch_params['select_end']

    # set up job config file and get values
    exit_code = 4
    job_params = get_job_params(job_cfg, exit_code)
    hd_log_dir = job_params['hd_log_dir']
    hive_script_dir = job_params['hive_script_dir'] 
    db_tgt = job_params['db_tgt']
    file_name = job_params['file_name']

    hive_ext = job_params['hive_ext']
    hive_table_items = job_params['hive_table_items']

    hive_table_items.sort()
    table_count = int(len(hive_table_items))
    for (key, val) in hive_table_items[int(restart_table_no):table_count]:
            values = val.split(',')
            values_len = len(values)
            hive_script = hive_script_dir + values[0] + hive_ext
            if int(values_len)== 2:
                hive_work_script = hive_script_dir + values[1] + hive_ext
            log_file_nm = values[0] + log_ext
            target_table =  str(values[0])
            log_file=job_log_dir+log_file_nm
            current_step=key

            # set up logging to file
            logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',filename=log_file,filemode='w')
            # define a Handler which writes INFO messages or higher to the sys.stderr
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            # set a format which is simpler for console use
            formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
            # tell the handler to use this format
            console.setFormatter(formatter)
            # add the handler to the root logger
            logging.getLogger('').addHandler(console)

            if int(restart_step_no) in (0,1):
                #Hive
                exit_code = 5
                restart_step = 1
                logging.info("================== Started Loading data to table : " + hive_script + " =============================")
                logging.info("> " + " ".join(map(str, sys.argv)))
                logging.info("log file: " + log_file)

                # log time of run
                logging.info('  run_time: ' + datetime.date.today().strftime('%a %Y-%m-%d [%j]'))

                # set defualt return code
                ret_code = 0

                # run hive job

                ret_code = queryWithDBParms(job_queue,db_tgt, file_name, select_start,select_end,ingestion_key,hive_script)

                logging.info('Hive script was successful for ' + hive_script)
                logging.info("================== END Hive script : " + hive_script + " =============================")

            #overwrite exit_code = 0. for sucessful execution
            exit_code = '0'
            load_log(exit_code)
            loggers = logging.getLogger('')
            handlers = list(loggers.handlers)
            for handler in handlers:
                loggers.removeHandler(handler)
                handler.flush()
                handler.close()

except SystemExit as e:  # log line of sys.exit(),
    err_msg = 'Job did not complete successfully: ' + traceback.format_exc(900)
    os.chdir(ORIGINAL_DIR)
    raise
except:  # catch ALL unexpected exceptions
    err_msg = 'Unexpected error: ' + traceback.format_exc(900)
    os.chdir(ORIGINAL_DIR)
    raise
os.chdir(ORIGINAL_DIR)
sys.exit(0)
