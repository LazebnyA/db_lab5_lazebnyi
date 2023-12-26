CREATE TABLE Patient
(
    patient_id INT NOT NULL,
    patient_name VARCHAR(30) NOT NULL,
    patient_age INT NOT NULL,
    patient_gender VARCHAR(15) NOT NULL,
    patient_blood_type VARCHAR(5) NOT NULL,
    patient_condition VARCHAR(15) NOT NULL,
    PRIMARY KEY (patient_id)
);

CREATE TABLE Hospital
(
    hospital_id INT NOT NULL,
    hospital_name VARCHAR(50) NOT NULL,
    PRIMARY KEY (hospital_id)
);

CREATE TABLE Patient_Admission
(
    date_of_admission DATE NOT NULL,
    patient_id INT NOT NULL,
    hospital_id INT NOT NULL,
    PRIMARY KEY (patient_id, hospital_id),
    FOREIGN KEY (patient_id) REFERENCES Patient(patient_id),
    FOREIGN KEY (hospital_id) REFERENCES Hospital(hospital_id)
);

CREATE TABLE Doctor
(
    doctor_id INT NOT NULL,
    doctor_name VARCHAR(30) NOT NULL,
    hospital_id INT NOT NULL,
    PRIMARY KEY (doctor_id),
    FOREIGN KEY (hospital_id) REFERENCES Hospital(hospital_id)
);

CREATE TABLE Patient_Doctor
(
    patient_id INT NOT NULL,
    doctor_id INT NOT NULL,
    PRIMARY KEY (patient_id, doctor_id),
    FOREIGN KEY (patient_id) REFERENCES Patient(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES Doctor(doctor_id)
);