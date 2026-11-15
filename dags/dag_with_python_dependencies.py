from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_python_dependencies_v02',
    default_args= default_args,
    start_date = datetime(2026, 10, 10, 2),
    schedule_interval = '@daily'
)
def library_order():

    @task
    def get_sklearn():
        import sklearn
        print(f'scikit-learn with version: {sklearn.__version__}')

    @task
    def get_matplotlib():
        import matplotlib
        print(f'matplotlib with version: {matplotlib.__version__}')

    get_matplotlib()
    get_sklearn()

lib_dag = library_order()