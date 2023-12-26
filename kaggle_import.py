import csv
import psycopg2

username = 'lazebnyi_oleksandr'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'healthcare_dataset.csv'

query_01 = '''
CREATE TABLE IF NOT EXISTS Patient
(
    patient_id INT NOT NULL,
    patient_name VARCHAR(30) NOT NULL,
    patient_age INT NOT NULL,
    patient_gender VARCHAR(15) NOT NULL,
    patient_blood_type VARCHAR(5) NOT NULL,
    patient_condition VARCHAR(15) NOT NULL,
    PRIMARY KEY (patient_id)
)
'''

query_02 = '''
CREATE TABLE IF NOT EXISTS Hospital
(
    hospital_id INT NOT NULL,
    hospital_name VARCHAR(50) NOT NULL,
    PRIMARY KEY (hospital_id)
)
'''

query_03 = '''
CREATE TABLE IF NOT EXISTS Patient_Admission
(
    date_of_admission DATE NOT NULL,
    patient_id INT NOT NULL,
    hospital_id INT NOT NULL,
    PRIMARY KEY (patient_id, hospital_id),
    FOREIGN KEY (patient_id) REFERENCES Patient(patient_id),
    FOREIGN KEY (hospital_id) REFERENCES Hospital(hospital_id)
)
'''

query_04 = '''
CREATE TABLE IF NOT EXISTS Doctor
(
    doctor_id INT NOT NULL,
    doctor_name VARCHAR(50) NOT NULL,
    hospital_id INT NOT NULL,
    PRIMARY KEY (doctor_id),
    FOREIGN KEY (hospital_id) REFERENCES Hospital(hospital_id)
)
'''

query_05 = '''
CREATE TABLE IF NOT EXISTS Patient_Doctor
(
    patient_id INT NOT NULL,
    doctor_id INT NOT NULL,
    PRIMARY KEY (patient_id, doctor_id),
    FOREIGN KEY (patient_id) REFERENCES Patient(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES Doctor(doctor_id)
)
'''


query_11 = '''
INSERT INTO Patient (patient_id, patient_name, patient_age, patient_gender, patient_blood_type, patient_condition)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (patient_id) DO NOTHING
'''

query_12 = '''
INSERT INTO Hospital (hospital_id, hospital_name)
VALUES (%s, %s)
ON CONFLICT (hospital_id) DO NOTHING
'''

query_13 = '''
INSERT INTO Patient_Admission (date_of_admission, patient_id, hospital_id)
VALUES (TO_DATE(%s, 'yyyy-mm-dd'), %s, %s)
ON CONFLICT (patient_id, hospital_id) DO NOTHING
'''

query_14 = '''
INSERT INTO Doctor (doctor_id, doctor_name, hospital_id)
VALUES (%s, %s, %s)
ON CONFLICT (doctor_id) DO NOTHING
'''

query_15 = '''
INSERT INTO Patient_Doctor (patient_id, doctor_id)
VALUES (%s, %s)
ON CONFLICT (patient_id, doctor_id) DO NOTHING
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    cur.execute(query_01)
    cur.execute(query_02)
    cur.execute(query_03)
    cur.execute(query_04)
    cur.execute(query_05)

    with open(INPUT_CSV_FILE, 'r') as hc_db_csv:
        reader = csv.DictReader(hc_db_csv)

        for idx, row in enumerate(reader, start=1):
            if idx == 9999:
                break

            # Import in Patient table block

            patient_id = 10000 + idx
            patient_name = row['Name']
            patient_age = row['Age']
            patient_gender = row['Gender']
            patient_blood_type = row['Blood Type']
            patient_condition = row['Medical Condition']

            patient_values = (patient_id, patient_name, patient_age, patient_gender, patient_blood_type, patient_condition)

            cur.execute(query_11, patient_values)

            # Import in Hospital table block

            hospital_id = 20000 + idx
            hospital_name = row['Hospital']

            hospital_values = (hospital_id, hospital_name)

            cur.execute(query_12, hospital_values)

            # Import in Patient_Admission block

            date_of_admission = row['Date of Admission']

            date_of_admission_values = (date_of_admission, patient_id, hospital_id)

            cur.execute(query_13, date_of_admission_values)

            # Import in Doctor block

            doctor_id = 30000 + idx
            doctor_name = row['Doctor']

            doctor_values = (doctor_id, doctor_name, hospital_id)

            cur.execute(query_14, doctor_values)

            # Import in Patient Doctor block

            patient_doctor_values = (patient_id, doctor_id)
            cur.execute(query_15, patient_doctor_values)

    conn.commit()


