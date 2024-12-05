# Set Up and Automate Your Data Pipeline in No Time Using Airflow and DuckDB

Dealing with data pipelines can be a real challenge. Manually extracting, transforming, and loading data often feels like piecing together a puzzle, one frustrating step at a time. It’s time-consuming, error-prone, and not exactly the most thrilling task in your day. But what if there was a better way?

This guide will show you how to automate your data pipelines using tools like Apache Airflow, DuckDB, and Daytona. Instead of wrestling with configurations and scripts, you’ll spend your time solving actual business problems. You’ll learn to extract data, apply transformations (like giving all your product prices a discount), and output clean, ready-to-use data without lifting a finger after setup.

By the end, you’ll have an automated pipeline and a clear understanding of how these tools work together. Whether it’s product data, sales analysis, or customer metrics, you’ll save time, reduce errors, and make your work much easier. Let’s dive in!

## Technologies Powering the ETL Pipeline

Let’s think of your [ETL pipeline](https://airflow.apache.org/use-cases/etl_analytics/) as a well-oiled team working together to get the job done quickly and smoothly. Here’s the dream team making it all happen:

- **[Apache Airflow](https://airflow.apache.org/)** – Manages and automates the entire pipeline process, making sure each task happens in the right order.

- **[DuckDB](https://duckdb.org/)** – A fast, in-memory database used to transform data, like applying discounts to product prices.

- **[Daytona](https://www.daytona.io/) and [Dev Containers](https://containers.dev/)** – Simplifies setup by providing a pre-configured development environment, saving you time on configuration.

## Turning Chaos into Automation: How the ETL Process Runs Like Clockwork

Imagine this: your data pipeline runs like clockwork, effortlessly handling everything while you sit back and focus on more important things. No more clicking through files, running scripts, or stressing over every little step. With Airflow and DuckDB, your data goes from raw to ready in no time, completely on autopilot.

Here’s the magic in three simple steps:

### 1. Extract 

The pipeline starts by reading data from a CSV file. In this case, it’s a file that contains product information, such as the product name, ID, and price.

### 2. Transform

After extracting the data, it’s sent to **DuckDB**, which performs the transformation. For this project, we’re applying a 10% discount to the product prices. DuckDB is used because it handles these transformations quickly and efficiently.

### 3. Load 

Once the transformation is complete, the new data with the discounted prices is written back to a new CSV file. This file can then be used for analysis or reporting.

And the best part? **Airflow** automates the timing and sequence of every step, so you don’t need to worry about running tasks manually. Just set it up once, and let Airflow handle the rest.

## What Makes This ETL Pipeline Stand Out

This ETL pipeline is built to make your data processing easier, faster, and more reliable. Here’s what sets it apart:

**1. Automated Task Management with Apache Airflow:** Apache Airflow takes care of scheduling and running each step in the pipeline. No need for manual work—Airflow ensures tasks run in the right order, every time.

**2. Quick and Efficient Transformations with DuckDB:** DuckDB handles data transformations quickly and easily. Whether it’s applying a discount or making complex changes, it works efficiently, even with large datasets.

**3. Ready-to-Use Sample Data:** The project comes with sample CSV files so you can jump right in. You can see how the pipeline works with real-world data without spending time creating your own test files.

**4. End-to-End Automation:** From reading raw data to applying transformations and saving the results, the entire pipeline runs automatically. You set it up once and let it handle the heavy lifting while you focus on other tasks.

**5. Hassle-Free Setup with Daytona:** Daytona automates the setup process, providing a ready-to-use development environment so you can start coding without worrying about configurations.

## Getting the ETL Pipeline Up and Running

We’ve set up everything for you, so you can skip the hassle and get started right away. With [Daytona](https://www.daytona.io/) handling the setup, you don’t need to worry about complicated configurations. Just jump in and start running your ETL pipeline with ease.

> The entire codebase for this ETL pipeline, including all configurations and scripts, is available on GitHub: https://github.com/daytonaio-experiments/starter-duckdb-airflow

![airflow duckdb working hld](/docs/assets/airflow-duckdb-hld.png)

### What You Need Before Starting

Make sure you have the following ready to go:

- **Daytona:** [Install Daytona](https://www.daytona.io/docs/installation/installation/) to handle the development environment setup.

- **Docker:** Docker should be installed and running to support the containerized environment.

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

## A Plug-and-Play Setup for Your ETL Pipeline

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
    "postCreateCommand": "pip install --upgrade pip && pip install apache-airflow pandas duckdb pyarrow && python3 -m venv airflow_venv"
}
```

Highlights of this Dev Container Configuration:

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

- **Post Create Command:** 
    - Upgrades `pip` and installs required Python packages (`apache-airflow`, `pandas`, `duckdb`, `pyarrow`).
    - Creates a virtual environment named `airflow_venv` for better package isolation.

With this setup, you can skip the hassle of installing dependencies or dealing with environment issues. The Dev Container takes care of everything, ensuring a smooth and consistent experience, whether you’re working on your own or collaborating with a team.

## Breaking Down the Code: How the ETL Pipeline Works

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

## Wrapping Up: What You’ve Achieved

Great job! You’ve successfully set up an ETL pipeline using Apache Airflow, DuckDB, and Daytona, and automated data transformations like a pro. Here’s a quick recap of what you’ve accomplished:

- **[Airflow](https://airflow.apache.org/)** - Orchestrated the entire ETL process, automating task scheduling and execution with ease.
- **[DuckDB](https://duckdb.org/)** - Provided a fast, in-memory SQL transformation layer to handle your data efficiently.
- **[Daytona](https://www.daytona.io/)** - Made the setup process a breeze, so you could skip the headache of configuring dependencies and focus on what matters: building your pipeline.

Thanks to Daytona, you avoided the usual setup roadblocks and were able to dive straight into working with your data. This powerful combination of tools makes automating your ETL tasks easier, faster, and more reliable.

As you move forward, feel free to explore and expand on this pipeline to suit your needs. And remember, if you hit any snags along the way, the [Daytona team](https://github.com/daytonaio/daytona) and their [community](https://go.daytona.io/slack) are always ready to lend a hand. Now, go ahead and experiment, optimize, and make your ETL pipeline even better. Happy coding!