import taosws

conn = None

try:
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host="localhost",
        port=6041,
    )

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    affectedRows = conn.execute_with_req_id(sql, req_id=1)
    print(f"inserted into {affectedRows} rows to power.meters successfully.")

    result = conn.query_with_req_id("SELECT ts, current, location FROM power.meters limit 100", req_id=2)
    num_of_fields = result.field_count
    print(num_of_fields)

    for row in result:
        print(row)

except Exception as err:
    print(err)
finally:
    if conn:
        conn.close()