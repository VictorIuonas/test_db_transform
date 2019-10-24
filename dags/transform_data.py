import psycopg2

def run():
    try:
        connect_str = "dbname='data' user='airflow' host='localhost' " + \
                      "password='airflow'"
        # use our connection values to establish a connection
        #conn = psycopg2.connect(connect_str)
        conn = psycopg2.connect(
            database="data",
            user="airflow",
            host="localhost",
            password="airflow",
            port=5432
        )
        # create a psycopg2 cursor that can execute queries
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        # create a new table with a single column called "name"
        # run a SELECT statement - no data in there, but we can try it
        cursor1.execute("""SELECT * from pipedrive_deal_activities""")
        # conn.commit() # <--- makes sure the change is shown in the database
        rows = cursor1.fetchall()
        for row in rows:
            print(row)
            cursor2.execute(
                """
...             INSERT INTO bi_sales_activity_query (activity_date, country, time_zone)
...             VALUES (%s, %s, %s);
...             """,
                ('', row[1], '')
            )
        cursor1.close()
        cursor2.close()
        conn.commit()
        conn.close()
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
