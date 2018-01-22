from pyhive import hive
import sys
import pprint
import os 
import logging
import pdb

def hive_con(host):
    conn = hive.connect(host).cursor()
    return conn

def create_hive_table(cursor):
    hquery = "create table temp8(id INT, name STRING, ciry STRING) \
             row format delimited fields terminated by ',' stored as textfile"
    htable = cursor.execute(hquery)    
    return "temp"

def load_hdfs_data(cursor, hdfspath, htable):
    hquery = "load data inpath " + hdfspath + " overwrite into table "+htable
    hload = cursor.execute(hquery)
    return hload
    
def load_local_data(cursor, hpath, htable):     
    hload = cursor.execute("load data local inpath " + hpath + " into table "+htable)
    return hload

def get_hive_data(cursor):
    cursor.execute('select * from siva')
    return cursor

if __name__ == '__main__':
 
   output = {}

   if '--help' in sys.argv:
       print "Usage python job.py <parameters> --host hostName"
       print "Parameters parsed:"
       print "\t --host : Hive host"

   else:

        try:
            cursor = hive_con(sys.argv[2])
            output['connection'] = "YES"
        except:
            logging.error("Error connecting to hive")
        

	htable = create_hive_table(cursor) 
     
        if 'local' in sys.argv[3]:
            load_local_data(cursor, sys.argv[4], htable)

        if 'hdfs' in sys.argv[3]:
	    load_hdfs_data(cursor, sys.argv[4], "temp")
