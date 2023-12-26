import psycopg2
import json

username = 'lazebnyi_oleksandr'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

json_data = {}

with conn:
    cur = conn.cursor()

    for table in ('patient', 'hospital', 'patient_admission', 'doctor', 'patient_doctor'):
        cur.execute(f'SELECT * FROM {table}')
        fields = [field[0] for field in cur.description]
        rows = [dict(zip(fields, row)) for row in cur]

        json_data[table] = rows

        with open('export_data.json', 'w') as file_to_write_in:
            json.dump(json_data, file_to_write_in, default=str)

