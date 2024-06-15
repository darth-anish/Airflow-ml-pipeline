# Airflow Machine Learning Pipeline

This project contains an Apache Airflow DAG that performs a machine learning pipeline using the Iris dataset loaded from a PostgreSQL database. The pipeline includes data loading, preprocessing, model training, and evaluation steps.
## Setup

### Installation

1. **Clone the Repository**:

    ```sh
    git clone https://github.com/your_username/airflow_project.git
    cd airflow_project
    ```

2. **Install Python Dependencies**:

    ```sh
    pip install -r requirements.txt
    ```

3. **Configure Airflow**:

   - Update `airflow.cfg` as needed.
   - Ensure `enable_xcom_pickling = True` in `airflow.cfg`.

4. **Initialize the Airflow Database**:

    ```sh
    airflow db init
    ```

5. **Create a Connection in Airflow for PostgreSQL**:

6. **Start the Airflow Web Server and Scheduler**:

    ```sh
    airflow webserver -p port_address
    airflow scheduler
    ```

## Running the DAG

The DAG `ml_pipeline_dag_v3` will automatically run based on the defined schedule interval. You can also trigger it manually from the Airflow UI.
