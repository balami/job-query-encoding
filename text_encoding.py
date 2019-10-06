import psycopg2 as pg
from psycopg2 import sql
from dbconnection import dbconnect

def get_col_datatypes():
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' GROUP BY table_name")
    results =  [r[0] for r in cur.fetchall()]
    for tbl in results:
        cur.execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = '%s'" % tbl)
        all_results = cur.fetchall()
        for item in all_results:
            tbl_name = item[0]
            col_name = item[1]
            d_type = item[2]
            if(d_type == 'text') or (d_type =='character varying'):
                cur.execute(sql.SQL("SELECT {} FROM {}").format(sql.Identifier(col_name.strip()),sql.Identifier(tbl_name.strip())))
                records =  [r[0] for r in cur.fetchall()]
                write_to_file(tbl_name,records)

def write_to_file(tbl_name,records):
    filename = tbl_name+'.txt'
    for record in records:
        if(record!=""):
            with open(filename, 'a') as the_file:
                the_file.write(format(record)+"\n")

if __name__ == "__main__":
    cur, conn = dbconnect()
    if(conn):
        get_col_datatypes()
