-- Даний цикл починаючи з 11 номеру вставляє до таблиці hospital тестові дані
-- включно до 20 номеру.

DO $$
DECLARE
    i INTEGER := 11;
BEGIN
    WHILE i <= 20 LOOP
        INSERT INTO hospital (hospital_id, hospital_name) 
        VALUES (20000 + i, 'hospital_name_' || i);

        i := i + 1;
    END LOOP;
END $$;
