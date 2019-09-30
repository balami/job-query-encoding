import psycopg2 as pg

def dbconnect():
    conn = pg.connect(user="", password="", host="", port="", database="")
    cur = conn.cursor()
    print("Connected to database\n")
    return cur, conn
