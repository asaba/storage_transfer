'''
Created on 01/feb/2016

@author: sabah
'''

import MySQLdb
from db_access import reader_user, reader_passwd


class CheckFits:
    def __init__(self):
        self.db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                                  user=reader_user,  # your username
                                  passwd=reader_passwd,  # your password
                                  db="srt_storage_dev")  # name of the data base

        # you must create a Cursor object. It will let
        #  you execute all the queries you need
        self.cur = self.db.cursor()

    def __enter__(self):
        return self

    def check(self, fullfitsfilepath):
        # Use all the SQL you like
        self.cur.execute("SELECT * FROM tdays WHERE filename='" + fullfitsfilepath + "'")

        if len(self.cur.fetchall()) > 0:
            return 1
        else:
            return 0

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()
