from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pendulum

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example']
)
def hello_world_dag():

    @task()
    def print_hello():
        print("Hello, Airflow!")
        return "Hello"

    @task()
    def log_info(input_from_hello):
        context = get_current_context()
        task_instance = context['task_instance']
        print(f"Received: {input_from_hello}")
        task_instance.xcom_push(key="my_key", value="some_value")
        print("Task instance ID:", task_instance.task_id)
        print("DAG ID:", task_instance.dag_id)
        print("Execution Date:", task_instance.execution_date)

        # Using Pendulum for current time
        current_time = pendulum.now("UTC")
        print("Current time:", current_time)

        # Pendulum makes time arithmetic easy
        tomorrow = current_time.add(days=1)
        print("This time tomorrow:", tomorrow)

    log_info(print_hello())

dag = hello_world_dag()