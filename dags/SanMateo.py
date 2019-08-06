"""

*NOTE* data_analytics folder path must be added to .bash_profile
"""
from datetime import datetime, date
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import send_csv
from IndigoAG import create_report


dag = DAG(
    "Indigo_AG_Report",
    description="Automated Report for San Mateo (36964, 37075)",
    schedule_interval="0 17 * * 4",
    start_date=datetime(2019, 8, 1),
    catchup=False,
)

create_csv = PythonOperator(
    task_id="create_csv", python_callable=create_report.main, dag=dag
)

today = date.today()
file_name = "Indigo_report.csv"
message = """Here is the updated&nbsp;<strong>Indigo Survey Report<em>&nbsp;
                        </em></strong>created on&nbsp;
                    <strong>{}</strong>.</span></span><br />""".format(
    today
)
subject = "Indigo Report " + str(today)
send_csv = PythonOperator(
    task_id="send_csv",
    python_callable=send_csv.main,
    op_kwargs={"file_name": file_name, "message": message, "subject": subject, "receiver":"ericlee@tinypulse.com"},
    dag=dag,
)

# def print_hello():
#     return "Hello world!"


# hello_operator = PythonOperator(
#     task_id="hello_task", python_callable=print_hello, dag=dag
# )

create_csv >> send_csv
# hello_operator >> create_csv >> email_csv
