# -*- coding: utf-8 -*-
__author__ = 'Vitaliy.Burkut'
#  libs
from datetime import datetime, timedelta
import os
from os.path import basename
import sys
from optparse import OptionParser
import logging

import pyodbc

from shutil import copyfile, move


########################################################################################################################
############################### Constants ##############################################################################
########################################################################################################################
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
NEW_DIR  = os.path.join(BASE_DIR, "new")
RES_DIR = os.path.join(BASE_DIR, "results")
OLD_DIR  = os.path.join(BASE_DIR, "proccesed")
BAD_DIR  = os.path.join(BASE_DIR, "bad")
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

def create_empty_file(p_full_file_name):
    copyfile(EMPTY_DB_FULL_FN, p_full_file_name)



def close_all_connect(p_conn_list):
    for c in p_conn_list:
        c.close()


def clear_table(p_connect):
    p_connect.execute ("delete from table1 where id not like 'Flop_Tur%';")

def create_prefix_table(p_connect, p_key_list):
    comm = "create table keys()"

def db_file_isCorrect(p_new_file_name):
    nf_full_path = os.path.join(NEW_DIR, p_new_file_name)
    conn = open_access_conect(nf_full_path)
    logger.info("Checking file {0}:".format(nf_full_path))
    try:
        conn.execute('alter table table1 ADD PRIMARY KEY (ID);')
        conn.commit()
        logger.warn("PK was not exists. Created.")
        conn.close()
        return True
    except Exception as pe:
        conn.rollback()
        conn.close()
        if type(pe) == pyodbc.ProgrammingError and  pe.args[0] == "23000":
            logger.error("PK could not created. Error:{0}".format(pe.args[1]))
            return False
        elif type(pe) == pyodbc.Error and  pe.args[0] == "HY000":
            logger.info("PK is already exists. Error:{0}".format(pe.args[1]))
            return True
        else:
            logger.error("Error creating PK:{0}".format(pe.args[1]))
            return False

def check_dirs():
    for dn in [NEW_DIR, RES_DIR, OLD_DIR, BAD_DIR]:
        if not os.path.exists(dn):
            os.makedirs(dn)



def copy_date_to_new_file(p_orig_file_conn, p_dist_file, p_key_list):
    SIZE_OF_LIST_PART = 80
    for i in range(0, len(p_key_list), SIZE_OF_LIST_PART):
        if i == 0:
            comm = "select * into [;DATABASE={0}].table1 from table1 where {1};".format(p_dist_file, " or ".join(list(map(lambda x:"id like '{0}%'".format(x), p_key_list[i:i+SIZE_OF_LIST_PART]))))
        else:
            comm = "insert into [;DATABASE={0}].table1 select * from table1 where {1};".format(p_dist_file, " or ".join(list(map(lambda x:"id like '{0}%'".format(x), p_key_list[i:i+SIZE_OF_LIST_PART]))))
        logger.debug(comm)
        p_orig_file_conn.execute(comm)



    p_orig_file_conn.execute('alter table [{0}].table1 ADD PRIMARY KEY (ID);'.format(p_dist_file))
    logger.info("PK crated in file {0}".format(p_dist_file))
    p_orig_file_conn.commit();
    pass


def copy_date_to_old_file(p_orig_file_conn, p_old_file_conn, p_dist_file, p_key_list):
    SIZE_OF_LIST_PART = 80

    check_comlite = False
    while not check_comlite:
        try:
            crsr = p_old_file_conn.cursor()
            for tf in crsr.tables(tableType='TABLE'):
                if tf.table_name =='table1_buf':
                    p_old_file_conn.execute("drop table table1_buf;")
                    p_old_file_conn.commit()
            check_comlite = True
        except Exception as e:
            pass





    p_old_file_conn.execute('select * into table1_buf from table1 where 1=0;')
    try:
        p_old_file_conn.execute('alter table table1_buf ADD PRIMARY KEY (ID);')
    except pyodbc.ProgrammingError as pe:
        if pe.args[0] != "42S02":
            raise
    p_old_file_conn.commit();

    for i in range(0, len(p_key_list), SIZE_OF_LIST_PART):
##        if i == 0:
##            comm = "select * into [;DATABASE={0}].table1_buf from table1 where {1};".format(p_dist_file, " or ".join(list(map(lambda x:"id like '{0}%'".format(x), p_key_list[i:i+SIZE_OF_LIST_PART]))))
##        else:
        comm = "insert into [;DATABASE={0}].table1_buf select * from table1 where {1};".format(p_dist_file, " or ".join(list(map(lambda x:"id like '{0}%'".format(x), p_key_list[i:i+SIZE_OF_LIST_PART]))))
        logger.debug(comm)
        p_orig_file_conn.execute(comm)
    p_orig_file_conn.commit();


    p_old_file_conn.commit();


 ##   comm = "select * into table1_buf2 from table1_buf where id not in (select id from table1);"
    comm = "insert into table1 select * from table1_buf where id not in (select id from table1);"
    p_old_file_conn.execute(comm)
    p_old_file_conn.commit();

    p_old_file_conn.execute("drop table table1_buf;")
    p_old_file_conn.commit();





def create_result_files(p_new_file_name, p_list_of_keys, p_max_index  = 10):
    nf_full_path = os.path.join(NEW_DIR, p_new_file_name)
    conections = []

    conections.append(open_access_conect(nf_full_path))
    logger.info("Opened connect to db file {0}".format(nf_full_path))

    for i in range(1, p_max_index + 1):
        logger.info("Start processing partition {0}".format(i))
        fn = os.path.join(RES_DIR, "{0}-{1}.mdb".format(os.path.splitext(p_new_file_name)[0], str(i)))
        if not os.path.exists(fn):
            create_empty_file(fn)
            logger.info("New empty file was creted {0}".format(fn))
            copy_date_to_new_file(conections[0], fn, p_list_of_keys[i])
        else:
            logger.info("Old file found {0}".format(fn))
            conections.append(open_access_conect(fn))
            copy_date_to_old_file(conections[0], conections[i], fn, p_list_of_keys[i])


    return conections












########################################################################################################################
#####################################  Program  ########################################################################
########################################################################################################################

def main(argv):
    try:



        logFileName = os.path.join(BASE_DIR, appName + '.log')

        ##logging.basicConfig(filename=logFileName, level=logging.DEBUG, format='%(asctime)s %(message)s')



        hdlr = logging.FileHandler(logFileName)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        logger.info('Starting...')
        logger.info('Check dirs exists')
        check_dirs()

        #parser = OptionParser()
        #parser.add_option("-s", "--send", action="store_true", dest="msg_send", default=False, help="send result mail msg, defoult show only")

        #options, args = parser.parse_args()


        #confFileName =  os.path.join(BASE_DIR, appName + ".conf")


        #if os.path.isfile(confFileName):
        #    with open(confFileName) as f:
        #        q =f.readline().rstrip('\n')
        #        w = f.readline().rstrip('\n')


        logger.info('Check new files exists...')
        new_files = [f for f in os.listdir(NEW_DIR) if f.endswith(".mdb") if os.path.isfile(os.path.join(NEW_DIR, f))]

        if len(new_files) == 0:
            logger.info('New files not found')
            exit(0)
        logger.info('Found {0} files'.format(len(new_files)))
        test_db_file = new_files[0]

 ## building the collection of key by datas in dispatch correspondance.csv
        logger.info('Start building key dictionary by data in file {0}'.format(DISPATCH_DICT_FILE))
        f = open(DISPATCH_DICT_FILE,'r')
        lines = f.readlines()[2:]
        f.close()
        list_of_keys = []



        for l in lines:
            k,v = l.split(';')

            if int(v) > len(list_of_keys) - 1:
                while int(v) > len(list_of_keys) -1:
                    list_of_keys.append (['Flop_Tur']) ## Flop_Tur prefix add to all lists
            if k not in list_of_keys[int(v)]:
                list_of_keys[int(v)].append(k)
        logger.info('End building key dictionary')

        all_conn = []
        for nf in new_files:
            logger.info('Start processing the file {0}'.format(nf))
            if db_file_isCorrect(nf):
                all_conn = create_result_files(nf, list_of_keys)
                close_all_connect(all_conn)
                move(os.path.join(NEW_DIR, nf), os.path.join(OLD_DIR, nf))
            else:
                move(os.path.join(NEW_DIR, nf), os.path.join(BAD_DIR, nf))






    except Exception as e:
        logger.error('Error type %s : %s', str(type(e)), str(e))
        close_all_connect(all_conn)
        raise(e)

    logger.info('Ending...')
    logging.shutdown()


if __name__ == "__main__":
        sys.exit(main(sys.argv))
