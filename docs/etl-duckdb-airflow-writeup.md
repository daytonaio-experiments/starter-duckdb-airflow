# Automate Your Data Pipeline: A Step-by-Step Guide Using Airflow and DuckDB

Dealing with data can be a real headache. Extracting it from one place, transforming it in another, and then loading it into a final format can quickly turn into a long, tedious process. Whether you’re working with product data, sales figures, or customer records, handling all that manually takes time, and let’s be honest it’s not the most exciting part of your day.

But what if you could automate it all? What if setting up an efficient [ETL pipeline](https://airflow.apache.org/use-cases/etl_analytics/) was as simple as following a few steps? That’s exactly what we’re going to do in this guide. You’ll learn how to create an ETL pipeline using **Apache Airflow** and **DuckDB** to automate the process of extracting data from a CSV, applying a simple transformation (like applying a 10% discount to product prices), and then loading it into a new CSV file.

By the end of this guide, you’ll understand how each part of the pipeline works, and you’ll see just how easy it is to set up and run it with the help of Daytona. [Daytona](https://www.daytona.io/) will simplify the environment setup, taking care of the messy configuration, so you can focus on what really matters: building and running your ETL pipeline without all the hassle.

Ready to make data handling effortless? Let’s dive in!

## Technologies Powering the ETL Pipeline

This project brings together a few key technologies that make building and running the ETL pipeline easy and efficient. Here’s what powers the pipeline:

- **[Apache Airflow](https://airflow.apache.org/)** – Manages and automates the entire pipeline process, making sure each task happens in the right order.

- **[DuckDB](https://duckdb.org/)** – A fast, in-memory database used to transform data, like applying discounts to product prices.

- **[Daytona](https://www.daytona.io/) and [Dev Containers](https://containers.dev/)** – Simplifies setup by providing a pre-configured development environment, saving you time on configuration.

## How the ETL Process Runs Automatically

Picture this: your data pipeline runs smoothly without you lifting a finger. No more manually opening files, running scripts, or tracking every step. With Airflow and DuckDB, you can automate the entire process—from extracting data to transforming it and finally loading it back into a new file.

Here’s how it works:

### 1. Extract 

The pipeline starts by reading data from a CSV file. In this case, it’s a file that contains product information, such as the product name, ID, and price.

**Example:** Input (`products.csv`):

| id | product_name | price |
|----|--------------|-------|
| 1  | Product A    | 100   |
| 2  | Product B    | 200   |
| 3  | Product C    | 150   |
| 4  | Product D    | 50    |
| 5  | Product E    | 80    |

### 2. Transform

After extracting the data, it’s sent to **DuckDB**, which performs the transformation. For this project, we’re applying a 10% discount to the product prices. DuckDB is used because it handles these transformations quickly and efficiently.

### 3. Load 

Once the transformation is complete, the new data with the discounted prices is written back to a new CSV file. This file can then be used for analysis or reporting.

**Example:** Output (`discounted_products.csv`):

| id | product_name | discounted_price |
|----|--------------|------------------|
| 1  | Product A    | 90.0             |
| 2  | Product B    | 180.0            |
| 3  | Product C    | 135.0            |
| 4  | Product D    | 45.0             |
| 5  | Product E    | 72.0             |


And the best part? **Airflow** automates the timing and sequence of every step, so you don’t need to worry about running tasks manually. Just set it up once, and let Airflow handle the rest.

## What Makes This ETL Pipeline Stand Out

This ETL pipeline comes with a set of features that make it easy to use and efficient. Here’s what you can expect:

**1. Task Automation with Apache Airflow:** Apache Airflow schedules and automates the tasks in your pipeline, ensuring they run in the right order without manual intervention.

**2. Fast Data Transformation with DuckDB:** DuckDB is used for fast, in-memory data transformations, applying changes like discounts efficiently, even with large datasets.

**3. Pre-loaded Sample Data:** The project includes sample CSV data, so you can test the pipeline right away and see how it processes real-world information.

**4. Full Pipeline Automation:** From extraction to transformation and loading, the pipeline runs end-to-end automatically, allowing you to focus on other tasks while it works.

**5. Simplified Setup with Daytona:** Daytona automates the setup process, providing a ready-to-use development environment so you can start coding without worrying about configurations.

## Getting the ETL Pipeline Running in No Time

We’ve already set up a pre-configured environment with an ETL pipeline, so you can get started quickly. [Daytona](https://www.daytona.io/) will handle the setup, letting you skip complex configurations and dive right into running your pipeline.

> The entire codebase for this ETL pipeline, including all configurations and scripts, is available in our GitHub repository: https://github.com/daytonaio-experiments/starter-duckdb-airflow

### Prerequisites

Before we start, make sure you have the following installed and ready to go:

- **Daytona:** Ensure Daytona is installed on your machine to handle the development environment setup.

- **Docker:** Docker should be installed and running, as it’s essential for the containerized environment.

- **Daytona Server:** Start the Daytona server by running the command `daytona serve` in your terminal.

### Setting Up the Development Environment

To get the development environment up and running with Daytona, follow these steps:

**Create the Daytona Workspace**

Run this command to create your Daytona workspace and clone the project repository:

```bash
daytona create https://github.com/daytonaio-experiments/starter-duckdb-airflow.git
```

This command will open the repository in a pre-configured workspace with all the necessary dependencies and settings. Daytona takes care of the environment setup, ensuring everything is ready to run the ETL pipeline with no additional configuration required.

### Running the ETL Pipeline with Just a Few Commands

Here are the next steps to get the ETL pipeline up and running:

**1. Activate the Virtual Environment**

First, ensure the necessary dependencies are installed in a dedicated environment. Activate the virtual environment by running the following command:

```bash
source airflow_venv/bin/activate
```
This ensures all required packages for Airflow and other dependencies are available.

**2. Initialize the Airflow Database**

Now, set up the Airflow database by running the following command:

```bash
airflow db init
```
This will initialize the Airflow metadata database, which Airflow uses to track and manage the pipeline's state.

**3. Create an Airflow Admin User**

To access the Airflow Web UI, you need to create an admin user. Run the following command to create the user (feel free to change the credentials as needed):

```bash
airflow users create \
    --role Admin \
    --username admin \
    --email admin \
    --firstname admin \
    --lastname admin \
    --password admin
```
This will create an Airflow admin account that you'll use to log in to the Airflow Web UI.

**4. Start the Airflow Scheduler**

The [Airflow scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html) manages the execution of tasks in your pipeline. Start it by running the following command:

```bash
airflow scheduler
```
This will begin the task scheduling process, making sure that your ETL tasks run according to your defined DAG.

**5. Start the Airflow Web Server**

In a separate terminal (after activating the virtual environment), start the Airflow web server:

```bash
airflow webserver -p 8080
```

Once the web server is running, open your browser and navigate to http://localhost:8080. Log in with the admin credentials you just created.

![Apache Airflow Login](/docs/assets/airflow-login-image.png)

Wait for a minute or two for the DAGs (Directed Acyclic Graphs) to load. Once they’re ready, you’ll see the `etl_duckdb_pipeline` in the list.

**Run the DAG**

To start the pipeline, simply click the **Play button** next to the `etl_duckdb_pipeline` DAG. This triggers the ETL process, which will automatically handle data extraction, transformation, and loading.

Once the DAG starts, you’ll see it executing the tasks in the correct order.

![Airflow DAG Pipeline Success](/docs/assets/airflow-dag-pipeline-success.png)

**View the Output**

After the pipeline finishes running, you can check the output by looking at the newly created `output/discounted_products.csv` file. To view its contents, run:

```bash
cat output/discounted_products.csv
```

This will display the transformed data, confirming that the pipeline has successfully applied the 10% discount and generated the new CSV.

## Dev Container Configuration: Simplifying the Development Setup

A key feature of this project is the use of a Dev Container configuration, which simplifies the development environment and ensures consistency across machines. Here’s a quick overview:

```json
{
    "name": "ETL Pipeline with Airflow and DuckDB",
    "image": "ubuntu:22.04",
    "features": {
        "ghcr.io/devcontainers/features/common-utils:2.5.2": {
            "username": "daytona",
            "userUid": 1000,
            "userGid": 1000,
            "configureZshAsDefaultShell": true
        },
        "ghcr.io/devcontainers/features/git:1": {},
        "ghcr.io/devcontainers/features/python:1.6.5": {
            "version": "3.10",
            "installPip": true
        }
    },
    "overrideFeatureInstallOrder": [
        "ghcr.io/devcontainers/features/common-utils",
        "ghcr.io/devcontainers/features/git",
        "ghcr.io/devcontainers/features/python"
    ],
    "workspaceFolder": "/workspace/starter-duckdb-airflow",
    "customizations": {
        "vscode": {
            "extensions": [
                "ms-python.python",
                "ms-python.vscode-pylance",
                "ms-toolsai.jupyter"
            ]
        }
    },
    "portsAttributes": {
        "8080": {
            "label": "Airflow Web UI",
            "onAutoForward": "notify"
        },
        "8793": {
            "label": "Airflow Scheduler",
            "onAutoForward": "notify"
        }
    },
    "containerEnv": {
        "AIRFLOW_HOME": "/workspace/starter-duckdb-airflow"
    },
    "remoteUser": "daytona",
    "postCreateCommand": "pip install --upgrade pip && pip install apache-airflow pandas duckdb pyarrow"
}
```

- **Base Image:** We use **Ubuntu 22.04** as the base image, providing a stable and familiar environment for running our ETL pipeline with Airflow and DuckDB.

- **Features and Tools:**

    - **Common Utils:** This feature installs essential utilities like `sudo`, `curl`, and `wget`, and configures **Zsh** as the default shell. It sets up the user daytona with UID and GID 1000, ensuring smooth operations.
    - **Git:** Version control is essential for managing code changes. Git is installed to handle versioning and collaboration seamlessly.
    - **Python:** The configuration includes Python 3.10 along with `pip` for managing dependencies. We also ensure the latest versions of required packages are available.

- **Workspace Folder:** The workspace is set to `/workspace/starter-duckdb-airflow`, ensuring that the code and all necessary files are properly located within the container.

- **VSCode Customizations:** Pre-configured extensions for **Python**, **Pylance**, and **Jupyter** are installed, making the development experience smoother with better code suggestions and support for notebooks.

- **Ports and Network Settings:**

    - Port **8080** is forwarded for the **Airflow Web UI**, which you can access to monitor your pipelines.
    - Port **8793** is used for the **Airflow Scheduler**, allowing you to manage task execution.

- **Container Environment:** The **AIRFLOW_HOME** environment variable is set to the workspace directory, ensuring that Airflow runs from the correct location.

- **Post-Create Commands:** After the container is created, it automatically upgrades pip and installs necessary Python libraries: **Apache Airflow**, **Pandas**, **DuckDB**, and **PyArrow**.

With this setup, you don’t need to manually install dependencies or worry about environment differences. The Dev Container handles everything, providing a seamless, consistent experience whether you're working alone or with a team.

## Exploring the Code: How the ETL Pipeline Comes Together

Let's take a closer look at the core parts of the code that make up this ETL pipeline. We’ll break down the key files and explore how everything works together, from defining tasks in Airflow to integrating DuckDB for data transformation.

### 1. DAG Definition: Orchestrating ETL Tasks with Airflow

The first step in building an ETL pipeline with Airflow is defining the [Directed Acyclic Graph (DAG)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html). The DAG is essentially a workflow that ties together all the tasks of your pipeline. Here's the core structure of the `etl_duckdb_pipeline.py` file:

```py
dag = DAG(
    'etl_duckdb_pipeline',
    description='Simple ETL Pipeline with DuckDB',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2024, 11, 29),
    catchup=False,
)
```

The DAG is named `etl_duckdb_pipeline` and is set to be manually triggered. The `schedule_interval=None` tells Airflow that this DAG doesn't run on a set schedule; it only runs when manually triggered. The `start_date` indicates when the DAG should start running.

### 2. ETL Tasks in Python: Extract, Transform, Load

Next, we’ll look at the individual tasks that make up the ETL pipeline. These tasks are defined as Python functions and connected to Airflow operators for execution.

- **Extract Data:** The first task, `extract_data()`, reads the product data from a CSV file and stores it in a pandas DataFrame.

```py
def extract_data():
    input_file = '/workspace/starter-duckdb-airflow/data/products.csv'
    df = pd.read_csv(input_file)
    print("Extracted Data:\n", df.head())
    return df
```

- **Transform Data:** The `transform_data()` task is responsible for applying a 10% discount to the prices. It takes the DataFrame from the previous task, connects to DuckDB, and performs the transformation using SQL.

```py
def transform_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='extract_data')
    con = duckdb.connect()
    con.register('products', df)
    transformed_df = con.execute("""
        SELECT id, product_name, price * 0.9 AS discounted_price
        FROM products
    """).df()
    print("Transformed Data:\n", transformed_df.head())
    return transformed_df
```

- **Load Data:** The final task, `load_data()`, takes the transformed data and saves it to a new CSV file.

```py
def load_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    if isinstance(df, pd.DataFrame):
        output_file = '/workspace/starter-duckdb-airflow/output/discounted_products.csv'
        df.to_csv(output_file, index=False)
        print(f"Transformed data saved to {output_file}")
    else:
        print(f"Error: Expected DataFrame but got: {type(df)}")
```

### 3. DuckDB Integration: Efficient Data Transformation

[DuckDB](https://duckdb.org/docs/guides/python/execute_sql.html) is an in-memory database that’s great for running analytical queries. In this project, DuckDB is used in the transformation step to apply a 10% discount to the prices. Here's how it fits in:

- DuckDB is connected to the pandas DataFrame, allowing us to run SQL queries directly on the data.
- The SQL query takes the existing product prices and multiplies them by 0.9 to apply the discount.
- The result is then converted back into a pandas DataFrame for further processing.

By using DuckDB, the transformation step is both fast and efficient, as it handles the in-memory operations without the need for external databases.

### 4. Airflow Operator Usage: Running Tasks Automatically

In Airflow, we use [operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) to define what each task should do. In this case, we use the `PythonOperator` to run the Python functions we defined earlier. Here’s how the operators are set up:

```py
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)
```

Each operator is tied to one of the ETL tasks, specifying the function to call when the task is executed. The `provide_context=True` ensures that the tasks can pass data between each other using [Airflow’s XCom feature](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html).

## Conclusion and Key Takeaways

Great job! You’ve successfully set up an ETL pipeline using Apache Airflow, DuckDB, and Daytona, and automated data transformations like a pro. Here’s a quick recap of what you’ve accomplished:

- **[Airflow](https://airflow.apache.org/)** - Orchestrated the entire ETL process, automating task scheduling and execution with ease.
- **[DuckDB](https://duckdb.org/)** - Provided a fast, in-memory SQL transformation layer to handle your data efficiently.
- **[Daytona](https://www.daytona.io/)** - Simplified environment setup, allowing you to get everything up and running without dealing with dependency headaches.

With Daytona’s help, you were able to skip the typical setup struggles and focus on building your pipeline. The combination of these tools lets you automate and simplify your ETL tasks, transforming your data faster and more efficiently.

As you move forward, feel free to explore and expand on this pipeline to suit your needs. And remember, if you hit any snags along the way, the [Daytona team](https://github.com/daytonaio/daytona) and their [community](https://go.daytona.io/slack) are always ready to lend a hand. Now, go ahead and experiment, optimize, and make your ETL pipeline even better. Happy coding!