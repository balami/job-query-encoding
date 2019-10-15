import psycopg2 as pg
import os
from psycopg2 import sql
from dbconnection import dbconnect
from collections import defaultdict

def get_col_datatypes():
    all_tbls = []
    tbl_dict = {}
    col_rem_list = ('md5sum','phonetic_code','name_pcode_cf','gender')
    col_type_check = ('text','character varying')
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' GROUP BY table_name")
    results =  [r[0] for r in cur.fetchall()]
    for tbl in results:
        cur.execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = '%s'" % tbl)
        all_results = cur.fetchall()
        for item in all_results:
            tbl_name = item[0]
            col_name = item[1]
            d_type = item[2]
            if((d_type in col_type_check) and (col_name not in col_rem_list)):
                if((tbl_name =='movie_info_idx') and (col_name =='info')):
                    pass
                else:
                    all_tbls.append([tbl_name,col_name])
    for key, val in all_tbls:
        tbl_dict.setdefault(key, []).append(val)
    for tbl, cols in tbl_dict.items():
        for col in cols:
            cur.execute(sql.SQL("SELECT {} FROM {}").format(sql.Identifier(col.strip()),sql.Identifier(tbl.strip())))
            records =  [r[0] for r in cur.fetchall()]
            write_to_file(records)
        os.system ("bash -c 'truncate -s-1 out.txt'")
        os.system ("bash -c 'echo "">> out.txt'")

def write_to_file(records):
    for record in records:
        if(record!="") and (record!='None') and (record is not None):
            with open('case2.txt', 'a', encoding="utf-8") as the_file:
                the_file.write(format(record)+",")

if __name__ == "__main__":
    cur, conn = dbconnect()
    if(conn):
        get_col_datatypes()
