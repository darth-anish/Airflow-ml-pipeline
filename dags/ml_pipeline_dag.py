import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG 
from airflow.decorators import task,dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score
import pickle 

# define default DAG arguments
default_args = {
    'owner':'darth',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id = 'ml_pipeline_dag_v3',
    start_date=datetime(2024,6,15),
    default_args = default_args,
    description = 'A machine learning pipeline with Iris dataset',
    schedule_interval='@daily',   
)
def run_ml_pipeline():       
    """
    Executes a machine learning pipeline for the Iris dataset.

    The pipeline consists of the following steps:
        1. Load data from a PostgreSQL database.
        2. Preprocess the data by splitting it into training and testing sets.
        3. Train a Decision Tree Classifier on the training data.
        4. Evaluate the trained model on the testing data and print the accuracy.

    Steps:
    - `load_data`: Connects to the PostgreSQL database using a PostgresHook and retrieves the Iris dataset.
    - `preprocess_data`: Splits the dataset into training and testing sets, returning them as dictionaries.
    - `train_model`: Trains a Decision Tree Classifier on the training data and returns the serialized model.
    - `evaluate_model`: Evaluates the serialized model on the testing data, printing and returning the accuracy.

    Returns:
        None
    """
     
    @task()
    def load_data():
        # Connect to PostgreSQL using PostgresHook
        hook = PostgresHook(postgres_conn_id='ml_iris')
        sql = "SELECT * FROM iris"
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        # Convert to DataFrame and return it
        data = pd.DataFrame(rows, columns=columns)
        return data

    @task(multiple_outputs=True)
    def preprocess_data(df):
        X = df.drop('species',axis=1)
        y = df['species']
        X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2,random_state=42,stratify=y)
        return {
            'X_train': X_train.to_dict(orient='list'),
            'X_test': X_test.to_dict(orient='list'),
            'y_train': y_train.to_list(),
            'y_test': y_test.to_list()
        }

    @task()
    def train_model(X_train,y_train):
        X_train = pd.DataFrame(X_train)
        model = DecisionTreeClassifier()
        model.fit(X_train,y_train)
        return pickle.dumps(model)

    @task()
    def evaluate_model(model, X_test, y_test):
        # Convert dictionaries back to DataFrames
        X_test = pd.DataFrame(X_test)
        # unload the pickle object
        model = pickle.loads(model)
        # evaluate the model
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test,y_pred)
        print(f"Model accuracy: {accuracy}")
        return accuracy

    # define task dependencies
    data = load_data()
    preprocessed_data = preprocess_data(data)
    model = train_model(preprocessed_data['X_train'], preprocessed_data['y_train'])
    evaluate = evaluate_model(model, preprocessed_data['X_test'], preprocessed_data['y_test'])

run_ml_pipeline()