import psycopg2
import matplotlib.pyplot as plt

username = 'lazebnyi_oleksandr'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

view_1 = '''
CREATE OR REPLACE VIEW CountDisease as 
    select patient_condition, count(*) from patient
        group by patient_condition;

select * from CountDisease
'''


view_2 = '''
CREATE OR REPLACE VIEW CountBloodTypes as
    select patient_blood_type, count(patient_blood_type)
    from patient
        group by patient_blood_type;

select * from CountBloodTypes
'''

view_3 = '''
CREATE OR REPLACE VIEW CountPatientsByYear as
    select EXTRACT(YEAR FROM date_of_admission) as year_of_admission, count(*) from patient_admission 
    join patient using(patient_id)
        group by EXTRACT(YEAR FROM date_of_admission)
        order by year_of_admission;

select * from CountPatientsByYear
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    cur.execute(view_1)

    diseases = []
    total_diseases = []

    for row in cur:
        diseases.append(row[0])
        total_diseases.append(row[1])

    x_range = range(len(diseases))

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3, figsize=(12, 4))
    bar_ax.bar(x_range, total_diseases, label='Total')
    bar_ax.set_title('Кількість людей хворих на хворобу', fontsize=8)
    bar_ax.set_xlabel('Хвороби')
    bar_ax.set_ylabel('Кількість')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(diseases, fontsize=6)

    cur.execute(view_2)

    blood_types = []
    total_blood_types = []

    for row in cur:
        blood_types.append(row[0])
        total_blood_types.append(row[1])

    pie_ax.set_title('Кількість людей з групою крові', fontsize=8)
    pie_ax.pie(total_blood_types, labels=blood_types, autopct='%1.1f%%', textprops={'fontsize': 6})

    cur.execute(view_3)
    years = []
    quantity = []

    for row in cur:
        quantity.append(row[0])
        years.append(row[1])

    graph_ax.plot(quantity, years, marker='o')
    graph_ax.set_xlabel('Рік')
    graph_ax.set_ylabel('Кількість пацієнтів')
    graph_ax.set_title('Графік залежності кількості пацієнтів від року', fontsize=8)
    graph_ax.set_yticks(range(5))


    for qnt, price in zip(quantity, years):
        graph_ax.annotate(price, xy=(qnt, price), xytext=(7, 2), textcoords='offset points')

mng = plt.get_current_fig_manager()

plt.show()