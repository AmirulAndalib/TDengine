import taos

conn = None
try:
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata")

    result = conn.query("SELECT ts, current, location FROM power.meters limit 100")
    print(result)
    # Get fields from result
    fields = result.fields
    for field in fields:
        print(field)

    # Get data from result as list of tuple
    data = result.fetch_all()
    for row in data:
        print(row)

except Exception as err:
    print(err)
finally:
    if conn:
        conn.close()