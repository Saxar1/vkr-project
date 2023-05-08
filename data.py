import psycopg2

conn = psycopg2.connect("""
    host=rc1b-575k77ibax2ssypu.mdb.yandexcloud.net
    port=6432
    sslmode=verify-full
    dbname=db1
    user=saxarok
    password=saxarok1
    target_session_attrs=read-write
""")

q = conn.cursor()
q.execute('SELECT version()')

print(q.fetchone())

conn.close()
