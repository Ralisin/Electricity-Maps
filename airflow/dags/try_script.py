from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "spark_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # nessuna schedulazione automatica
    catchup=False
) as dag:
    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="docker exec spark-master spark-submit --master spark://spark-master:7077 /app/job.py"
    )
