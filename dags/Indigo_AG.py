"""

data_analytics folder path must be added to .bash_profile
"""
import sys
from datetime import datetime, date
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import send_csv

# from IndigoAG import create_report


dag = DAG(
    "Indigo_AG_Report",
    description="Automated Report for Indigo AG (32995)",
    schedule_interval="0 17 * * 2",
    start_date=datetime(2019, 7, 9),
    catchup=False,
)

create_csv = BashOperator(
    task_id="create_csv",
    bash_command="python ~/airflow/reports/IndigoAG/create_report.py",
    dag=dag,
)

today = date.today()
file_name = [
    "Indigo_report.csv",
    "boolean_pct.csv",
    "boolean_score.csv",
    "scale_pct.csv",
    "scale_score.csv",
]
message = """Here are the updated&nbsp;<strong>Indigo AG files<em>&nbsp;
                        </em></strong>created on&nbsp;
                    <strong>{}</strong>.</span></span><br />""".format(
    today
)
subject = "Indigo Report " + str(today)
send_csv = PythonOperator(
    task_id="send_csv",
    python_callable=send_csv.main,
    op_kwargs={
        "file_name": file_name,
        "message": message,
        "subject": subject,
        "receiver": "jennifer@tinypulse.com",
    },
    dag=dag,
)

create_csv >> send_csv