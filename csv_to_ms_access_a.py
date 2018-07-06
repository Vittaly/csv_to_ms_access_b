# -*- coding: utf-8 -*-
__author__ = 'Vitaliy.Burkut'

#################### version history ###########################################
################################################################################
# 0.1 Created       (very slow)
# 0.2 try with dbf (slow)
# 0.3 try withh direct csv read from access (fast)
################################################################################

###############################  libs  #########################################
#from datetime import datetime, timedelta
import os
from os.path import basename
import sys
import time
#from optparse import OptionParser
import logging

import pyodbc
#import dbf

from shutil import copyfile, move
import csv
from queue import Queue
from threading import Thread


########################################################################################################################
############################### Constants ##############################################################################
########################################################################################################################
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
NEW_DIR  = os.path.join(BASE_DIR, "new")
RES_DIR = os.path.join(BASE_DIR, "results")
OLD_DIR  = os.path.join(BASE_DIR, "proccesed")
BAD_DIR  = os.path.join(BASE_DIR, "bad")
TEMP_DIR  = os.path.join(BASE_DIR, "temp")
EMPTY_DB_FULL_FN = os.path.join(BASE_DIR, "empty_db_for_copy.mdb")
DISPATCH_DICT_FILE =  os.path.join(BASE_DIR, "dispatch correspondance.csv")


appName = os.path.splitext(basename(__file__))[0]
logger = logging.getLogger(appName)


########################################################################################################################
#############################  Functions  ##############################################################################
########################################################################################################################

def open_access_conect(p_file_name):
    conn_str = r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}}; DBQ={0}'.format(p_file_name)
    return pyodbc.connect(conn_str)

def create_empty_file_connect(p_full_file_name):
    copyfile(EMPTY_DB_FULL_FN, p_full_file_name)
    return  open_access_conect(p_full_file_name)


def create_table(p_connect):
    p_connect.execute ("create table table1 (id VARCHAR(20) PRIMARY KEY, Valeur1 VARCHAR(20), Valeur2 VARCHAR(20), Valeur3 VARCHAR(20), Valeur4 VARCHAR(20), Valeur5 VARCHAR(20), Valeur6 VARCHAR(20));")
    p_connect.commit()
    #PRIMARY KEY
def create_tmp_table(p_connect):
    try:
        p_connect.execute ("drop table tmp;")
    except Exception as e:
        logger.warn("Error when tried drop tmp table:{0}".format(e.args[1]))

    try:
        p_connect.execute ("create table tmp (id VARCHAR(20), Valeur1 VARCHAR(20), Valeur2 VARCHAR(20), Valeur3 VARCHAR(20), Valeur4 VARCHAR(20), Valeur5 VARCHAR(20), Valeur6 VARCHAR(20), pk AUTOINCREMENT PRIMARY KEY);")
    except Exception as e:
        if e.args[0] == '42S01':
            logger.warn("table TMP is already exists. Error:{0}".format(e.args[1]))

    try:
        p_connect.execute ("create index tmp_id_idx on tmp (id);")
    except Exception as e:
        if e.args[0] == 'HYS011':
            logger.warn("index tmp_id_idx is already exists. Error:{0}".format(e.args[1]))
    p_connect.commit()

def table_struct_isCorrect(p_conn):
    try:
        p_conn.execute('alter table table1 ADD PRIMARY KEY (ID);')
    except Exception as pe:
        if pe.args[0] == "42S02":
           logger.error('Table TABLE1 not exists in DB file')
           return False
        elif type(pe) == pyodbc.Error and  pe.args[0] == "HY000":
            logger.info("PK is already exists. Error:{0}".format(pe.args[1]))
            return True
        else:
            logger.Error(" Error:{0}".format(pe.args[1]))
            return False

    p_conn.commit();
    logger.warn('PK was not exists. Created')
    return True


def merga_data_in_mdb(p_conn):
    cur = p_conn.cursor()
    res = cur.execute('select count(*) from table1;')
    val = res.fetchone()
    old_row_count = val[0]
    logger.info('Rows count in table1 before merge: {0}'.format(val[0]))
    res = cur.execute('select count(*) from tmp;')
    val = res.fetchone()
    logger.info('Rows count in temp table before merge: {0}'.format(val[0]))
    res = cur.execute('select id, count(*) from tmp group by id having count(*) > 1;')
    val = res.fetchall()
    if val == None:
        logger.info('Duplicate IDs in additional rows not found')
    else:
        logger.warn('Duplicate IDs in additional rows was found')
        for r in val:
            logger.warn('Duplicated id in new data: id:{0} count:{1}'.format(r[0], r[1]))
            logger.warn('Deleting...')
            cur.execute('delete from tmp where id = ? and pk not in (select min(pk) from tmp tt where tt.id = ?);', r[0], r[0])
        p_conn.commit()


    cur.execute('select id from tmp t where t.id in (select id from table1);')

    for r in cur:
        logger.warn('In new data was found id that already exists in table1: {0}'.format(r.id))
    cur.execute('insert into table1 select id, Valeur1, Valeur2, Valeur3, Valeur4, Valeur5, Valeur6  from tmp t where t.id not in (select id from table1);')
    p_conn.commit()
    res = cur.execute('select count(*) from table1;')
    val = res.fetchone()
    new_row_count = val[0]
    logger.info('Rows count in table1 after merge: {0}. {1} rows has added'.format(new_row_count, new_row_count - old_row_count))
    p_conn.execute('drop table tmp;')
    p_conn.commit()








def write_rec_to_mdb(p_conn, p_tmp_tab_name):

    cmd = 'insert into tmp (id, Valeur1, Valeur2, Valeur3, Valeur4, Valeur5, Valeur6) select id, Valeur1, Valeur2, Valeur3, Valeur4, Valeur5, Valeur6  from [{0}] in "{1}"[Text;FMT=Delimited;HDR=YES];'.format(p_tmp_tab_name, TEMP_DIR)
    logger.debug('exeecute:{0}'.format(cmd))
    try:
        p_conn.execute(cmd)
    except pyodbc.ProgrammingError as pe:
        logger.error('Error then was insert into TABLE1: {0}'.format(pe))

    #for rec in p_records:
    #    cur.execute('insert into table1 values(?, ?, ?, ?, ?, ?, ?)', rec + [None for i in range(7 - len(rec))])
    p_conn.commit()
    logger.info("data from tmp table moved to mdb")


def write_rec_to_tmp_db(p_records, p_tmp_tab):
    logger.debug('start write to tmp tables')
    p_tmp_tab.writerows(p_records)
    logger.debug('end write to tmp tables')




def access_writer(p_csv_file_name, p_file_index, p_queue):
    logger.info("Start thread by processing partition {0} of {1}".format(p_file_index, len (DICT_OF_KEYS) - 1))
    fn = os.path.join(RES_DIR, "{0}-{1}.mdb".format(os.path.splitext(p_csv_file_name)[0], p_file_index))
    conn = None
    if not os.path.exists(fn):
        conn = create_empty_file_connect(fn)
        logger.info("New empty file was creted {0}".format(fn))
        create_table(conn)
    else:
        logger.info("File {0} was found".format(fn))
        conn =  open_access_conect(fn)
        logger.info("Connected. Check PK in TABLE1 from ")
        if not table_struct_isCorrect(conn):
            logger.error("Struct or data in file {0} is incorrect. Abort".format(fn))
            conn.close()
            return -1

    create_tmp_table(conn)
    logger.info("TMP table created")
    conn.autocommit = False

    tmp_tab_name = 't' + str(p_file_index) + '.csv'
    tmp_tab_file_name =  os.path.join(TEMP_DIR,  tmp_tab_name )
    tmp_tab_file = open (tmp_tab_file_name, 'w',newline='')
    tmp_table =  csv.writer (tmp_tab_file, quoting=csv.QUOTE_NONNUMERIC)
    tmp_table.writerow(['ID', 'Valeur1' , 'Valeur2' , 'Valeur3', 'Valeur4', 'Valeur5', 'Valeur6'])

    was_sleeped = False
    record_was_writed = False

    while True:
        recs_part = None
        if not p_queue.empty() or was_sleeped:
            recs_part = p_queue.get()
            was_sleeped = False
            if len(recs_part) == 0:
                logger.debug("Recived 0 records. Finalize")
                tmp_tab_file.close()
                logger.debug('temp csv file closed')
                write_rec_to_mdb(conn, tmp_tab_name)
                try:
                    merga_data_in_mdb(conn)
                except Exception as e:
                    logger.error(e)
                    logger.warn('Try again call merga_data_in_mdb')
                    try:
                        merga_data_in_mdb(conn)
                    except Exception as ee:
                        logger.error('Second error in merga_data_in_mdb:{0}'.format(ee))
                        return -1


                p_queue.task_done()
                conn.close()
                return
            else:
                logger.debug("Recived {0} records for save into {1}".format(len(recs_part), fn))
                write_rec_to_tmp_db(recs_part, tmp_table)
                record_was_writed = True
                p_queue.task_done()
                logger.debug("Steep of task done")

        else:
            logger.debug("queue is empty")
            if record_was_writed:
                logger.debug("temt table is not empty. Will write to mdb")
                tmp_tab_file.close()
                write_rec_to_mdb(conn, tmp_tab_name)
                logger.debug("clear temp table after write into mdb")
                tmp_tab_file = open (tmp_tab_file_name, 'w',newline='')
                tmp_table =  csv.writer (tmp_tab_file, quoting=csv.QUOTE_NONNUMERIC)
                tmp_table.writerow(['ID', 'Valeur1' , 'Valeur2' , 'Valeur3', 'Valeur4', 'Valeur5', 'Valeur6'])
                logger.debug("tmp db recreated")
                record_was_writed = False
                continue
            else:
                logger.debug('sleep')
                time.sleep(10)
                was_sleeped = True


def csv_file_isCorrect(p_csv_file_name):
    nf_full_path = os.path.join(NEW_DIR, p_csv_file_name)

    logger.info("Checking file {0}:".format(nf_full_path))
    first_line = ''
    with open(nf_full_path, "r") as f:
        first_line = f.readline()
    logger.debug('first line of file:{0}'.format(first_line))
    headers  =  first_line.split(',')
    if len(headers) < 8:
        logger.error('total count of headers is less then eight')
        return False
    if '_'.join(headers[0:3]) != "Flop_Turn_Hand":
        logger.error('Headers of first 3th fields is not "Flop", "Turn" and "Hand"')
        return False

    return True








def check_dirs():
    for dn in [NEW_DIR, RES_DIR, OLD_DIR, BAD_DIR, TEMP_DIR]:
        if not os.path.exists(dn):
            os.makedirs(dn)





def get_file_index_by_key(p_key):
    for i, l in enumerate(DICT_OF_KEYS):
        if p_key in l:
            return i

def get_tab_rec(p_csv_rec):
    res =  ('_'.join([p_csv_rec[0], p_csv_rec[1], p_csv_rec[2]]),) + tuple(f.replace('.', '') for f in p_csv_rec[7:len(p_csv_rec)-1])
    return res


def process_csv_file(p_csv_file_name):
    PART_SIZE = 30000
    QUEUE_SIZE = 10
    csv_full_name = os.path.join(NEW_DIR, p_csv_file_name)
    max_file_index = len(DICT_OF_KEYS) - 1
    fd = open(csv_full_name, 'r')
    csv_reader = csv.reader(fd)
    rec_dict = [[] for i in range(max_file_index)]
    queues = [Queue(QUEUE_SIZE) for i in range(max_file_index)]

    row_count  = 0
    row_w_error  = 0
    row_count_per_file = [0 for i in range(max_file_index )]
    threads = [Thread(target = access_writer, name = 'thread for file {0}'.format(i+1), args = (p_csv_file_name, i+1, queues[i]), daemon = True) for i in range(max_file_index)]
    for t in threads:
        t.start()
    for rec in csv_reader:
        file_index = get_file_index_by_key(rec[0])
        if file_index == None:
            logger.error('Could not define file index for the record. Ignored: {0}'.format(rec))
            row_w_error += 1
            continue
        if file_index == 0:
            for l in  rec_dict:
                l.append(get_tab_rec(rec))
        else:
            rec_dict[file_index - 1].append(get_tab_rec(rec))
            if len(rec_dict[file_index - 1]) == PART_SIZE:
                logger.debug('Put to queue {0} records for file #{1}'.format(len(rec_dict[file_index - 1]), file_index))
                queues[file_index - 1].put(rec_dict[file_index - 1])
                #access_writer(p_csv_file_name, file_index, queues[file_index -1])
                row_count_per_file[file_index - 1] += len(rec_dict[file_index - 1])
                rec_dict[file_index - 1] = []
        row_count  += 1

    for i in range(max_file_index):
        if len(rec_dict[i]) > 0:
            logger.debug('Put to queue {0} last part of records for file #{1}'.format(len(rec_dict[i]), i+1))
            queues[i].put(rec_dict[i])
            row_count_per_file[i] += len(rec_dict[i])
            rec_dict[i] = []


    for i, q in enumerate (queues):
        logger.debug('Put to queue empty list to stop for file #{0}'.format(i+1))
        q.put([]) # put the empty list for stop thread
        q.join() # wait to stop
    logger.info('-------------------------------------------------------------')
    logger.info('total record processing in file "{0}": {1}'.format(p_csv_file_name, row_count))
    logger.info('Rows with key define error: {0}'.format( row_w_error))
    for i ,v in  enumerate (row_count_per_file):
        logger.info('send to file {0}: {1}'.format(i + 1, v))
    logger.info('In summary:{0}'.format(sum(row_count_per_file)))
    logger.info('Compare with total line in file :{0}'.format(row_count == sum(row_count_per_file)))


















########################################################################################################################
#####################################  Program  ########################################################################
########################################################################################################################

def main(argv):
    try:



        logFileName = os.path.join(BASE_DIR, appName + '.log')

        ##logging.basicConfig(filename=logFileName, level=logging.DEBUG, format='%(asctime)s %(message)s')



        hdlr = logging.FileHandler(logFileName)
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        logger.info('Starting...')
        logger.info('Check dirs exists')
        check_dirs()




        logger.info('Check new files exists...')
        new_files = [f for f in os.listdir(NEW_DIR) if f.endswith(".csv") if os.path.isfile(os.path.join(NEW_DIR, f))]

        if len(new_files) == 0:
            logger.info('New files not found')
            exit(0)
        logger.info('Found {0} files'.format(len(new_files)))

 ## building the collection of key by datas in dispatch correspondance.csv
        logger.info('Start building key dictionary by data in file {0}'.format(DISPATCH_DICT_FILE))
        f = open(DISPATCH_DICT_FILE,'r')
        lines = f.readlines()[1:]
        f.close()



        global DICT_OF_KEYS
        DICT_OF_KEYS = []

        for l in lines:
            k,v = l.split(';')
            # if index (k) = 0 it is mean that records must be add to any file
            if int(v) > len(DICT_OF_KEYS) - 1:
                while int(v) > len(DICT_OF_KEYS) -1:
                    DICT_OF_KEYS.append ([]) ## init as empty list
            if k not in DICT_OF_KEYS[int(v)]:
                DICT_OF_KEYS[int(v)].append(k)
        logger.info('End building key dictionary')


        for nf in new_files:
            logger.info('Start processing the file {0}'.format(nf))
            if csv_file_isCorrect(nf):
                process_csv_file(nf)
                move(os.path.join(NEW_DIR, nf), os.path.join(OLD_DIR, nf))
            else:
                logger.error('file {0} if bad moving to dir {1}'.format(nf, BAD_DIR))
                move(os.path.join(NEW_DIR, nf), os.path.join(BAD_DIR, nf))
        logger.info('No more files. Stoping...')





    except Exception as e:
        logger.error('Error type %s : %s', str(type(e)), str(e))
        raise(e)

    logger.info('Ending...')
    logging.shutdown()


if __name__ == "__main__":
        sys.exit(main(sys.argv))
