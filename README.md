# Web Application to Visualize the Performance of Machine Learning Aided Pairs Trading Strategies

![Archutecture Diagram](https://i.ibb.co/F4GWF1g/Pairs-Trading-Architecture.png)

This repository is part of a project that aims to showcase the performance of a machine learning assisted pairs trading strategy. The project consists of 4 separate services in different repositories.

- [Orchestrator](https://github.com/kerem-kaynak/pairs-trading-orchestrator): Orchestrator of data pipelines and processing workflows. Runs scheduled ETL jobs.
- [Quant / ML Service](https://github.com/kerem-kaynak/pairs-trading-quant-service): Web server exposing endpoints to perform machine learning tasks.
- [Backend API](https://github.com/kerem-kaynak/pairs-trading-backend): Backend API serving data to the client.
- [Frontend](https://github.com/kerem-kaynak/pairs-trading-frontend): Frontend application for web access.

The research in the thesis leading to this project can be found [here](https://github.com/kerem-kaynak/pairs-trading-with-ml) with deeper explanations of the financial and statistical concepts.

## Pairs Trading Orchestrator

This service is the main supplier of all data for the whole project. It interacts with a third party data provider to extract bulk data, transform it and load to the project database. There are also workflows to compute outputs of models hosted in the ML service and write them to the database for persistency.

### Technologies

The service is built using Apache Airflow. Airflow utilizes DAGs (Directed Acyclic Graph) to define workflows and scheduled tasks. The database is a PostgreSQL database hosted on Google Cloud, using pg8000 connector. Data analysis and transformation libraries such as NumPy and Pandas are heavily used, as well as raw SQL scripts.

### Project Architecture

The project is structured in two main folders: `dags` and `models`. The `dags` folder consists of everything related to the workflows. DAGs and tasks making up DAGs are all defined in here. The folder is separated into layers, and for each layer there is a separate folder for each dag. Inside these folders are the DAG definitions, a `constants` file consisting of constants relevant to that specific DAG, backfill DAG definitions and optionally a `utils` folder for DAG specific utils. At the root of the `dags` folder, there's another `utils` folder for shared util functions across DAGs. The `models` folder contains SQL scripts defining the schema of each table in the database.

### Requirements

- Airflow 2.9.1+
- Python 3.10+
- Make

### Local Development

Install project dependencies:
```
pip install -r requirements.txt
```

Create a .env file and populate the following variables:
```
DB_USER=
DB_PASS=
DB_NAME=
DB_INSTANCE_CONNECTION_NAME=
POLYGON_API_KEY=
QUANT_SERVICE_API_TOKEN=
```

Run airflow standalone:
```
make run
```
