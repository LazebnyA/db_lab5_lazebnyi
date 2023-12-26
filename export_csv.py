import psycopg2

username = 'lazebnyi_oleksandr'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    for table in ('patient', 'hospital', 'patient_admission', 'doctor', 'patient_doctor'):
        query = f'COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER'
        with open(f'{table}.csv', 'w') as file_to_write_in:
            cur.copy_expert(query, file_to_write_in)

