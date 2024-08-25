from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.session import create_session
from datetime import datetime, timedelta
from dags.utils.database import get_connection, run_query
from dags.utils.logger import logger

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

@dag(
    'orchestrator_health_check',
    default_args=default_args,
    description='Advanced Airflow health check',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    is_paused_upon_creation=True
)
def orchestrator_health_check():

    @task
    def check_db_connection():
        try:
            run_query('SELECT 1')
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    @task
    def check_dag_integrity():
        dagbag = DagBag()
        if dagbag.import_errors:
            logger.error(f"DAG import errors: {dagbag.import_errors}")
            raise Exception("DAG import errors detected")
        logger.info("All DAGs loaded successfully")

    @task
    def check_failed_tasks():
        with create_session() as session:
            failed_tasks = session.query(TaskInstance).filter(
                TaskInstance.state == State.FAILED,
                TaskInstance.end_date >= datetime.now() - timedelta(days=1)
            ).count()
        
        if failed_tasks > 0:
            logger.error(f"There are {failed_tasks} failed tasks in the last 24 hours")
            raise Exception(f"There are {failed_tasks} failed tasks in the last 24 hours")
        logger.info("No failed tasks in the last 24 hours")

    @task
    def check_dag_runs():
        with create_session() as session:
            running_dags = session.query(DagRun).filter(
                DagRun.state == State.RUNNING,
                DagRun.start_date <= datetime.now() - timedelta(hours=6)
            ).count()
        
        if running_dags > 0:
            logger.error(f"There are {running_dags} DAGs running for more than 6 hours")
            raise Exception(f"There are {running_dags} DAGs running for more than 6 hours")
        logger.info("No long-running DAGs detected")

    check_disk_space = BashOperator(
        task_id='check_disk_space',
        bash_command='df -h | awk \'$NF=="/"{printf "Disk Usage: %d/%dGB (%s)\n", $3,$2,$5}\'',
    )

    check_airflow_logs = BashOperator(
        task_id='check_airflow_logs',
        bash_command='ls -lh $(dirname $(airflow info | grep "Airflow Home" | awk \'{print $4}\'))/logs | awk \'{print "Airflow logs size: " $5}\'',
    )

    check_scheduler = BashOperator(
        task_id='check_scheduler',
        bash_command='ps aux | grep "airflow scheduler" | grep -v grep || echo "Scheduler not running"',
    )

    check_db_connection() >> check_dag_integrity() >> check_failed_tasks() >> check_dag_runs() >> [check_disk_space, check_airflow_logs, check_scheduler]

dag = orchestrator_health_check()