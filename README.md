<div align="center">

## Simple Data Pipeline (GCloud)
#### To Read Dow Jones (DJI) Data in Parquet File and Append It Into MySQL in Google Cloud Storage at the End of Each Day Using Prefect v3.0

</div>

---

#### I. Overview
This project demonstrates how to build a simple data pipeline that reads the Dow Jones Industrial Average (^DJI) data from a Parquet file and appends it to a MySQL database hosted in Google Cloud Storage (GCS) at the end of each day. The pipeline utilizes the Prefect v3.0 orchestration tool to automate and monitor the process, showcasing efficient integration of data storage and cloud services.

---

#### II. Pipeline Flow
1. **Task 1: Read Data**
Read historical Dow Jones (^DJI) data stored in a Parquet file using the `polars` library.

2. **Task 2: Append Data**
Append the data to a MySQL database hosted in Google Cloud Storage.

3. **Task 3: Schedule Pipeline**
Ensure the pipeline runs daily to append new data automatically.

4. **Prefect Orchestration**
Each step of the pipeline is orchestrated using Prefect.

<div align="center">
  <img src="./documents/Check Server UI.png" alt="Prefect Logo" height="400"/>
  <img src="./documents/Check Pipeline Results.png" alt="Prefect Results" height="400"/>
</div>

---

#### III. Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/NolanMM/Simple_Prefect_v3.0_Data_Pipeline_GCS.git
    cd Simple_Prefect_v3.0_Data_Pipeline_GCS
    ```

2. Create and activate a virtual environment:
    ```
    python -m venv venv

    # Linux
    source venv/bin/activate 

    # Windows
    cd ./venv/Scripts
    activate
    ```

3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. Set up Google Cloud credentials:
    - Ensure you have a Google Cloud service account key file.
    - Export the key file for authentication:
      ```bash
      export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/keyfile.json"
      ```
      If you do not set up Mysql in Google Cloud, Please follow this instructions and set up the ```.env``` file:
        ```bash
        GOOGLE_APPLICATION_CREDENTIALS="path/to/your/keyfile.json"
        ```

---

#### IV. Usage

**1. Activate the Virtual Environment**
Open a separate terminal and activate the Python virtual environment:
```bash
# On Linux: source venv/bin/activate  
# On Windows: ./venv/Scripts/activate
```

**2. Start Prefect Server**
In the same terminal, start the Prefect server to enable orchestration and monitoring:
```bash
prefect server start
```
<div align="center">
  <img src="./documents/Run Server.png" alt="Prefect Logo" height="200"/>
</div>

This will launch the Prefect server locally. By default:
- The server UI is accessible at http://127.0.0.1:4200/dashboard
- Keep this terminal running during the execution of the pipeline.

**3. Run the Pipeline**
Open a new terminal, navigate to the project directory, activate the virtual environment,
```bash
# On Linux: source venv/bin/activate  
# On Windows: venv\Scripts\activate
```
and execute the pipeline:
```bash
$env:PREFECT_API_URL="http://127.0.0.1:4200/api"; python Simple_Data_Pipeline.py
```

<div align="center">
  <img src="./documents/Run Pipeline.png" alt="Prefect Logo" height="200"/>
</div>

**4. Monitor Pipeline Execution**
Visit the Prefect server UI in your browser (http://127.0.0.1:4200/dashboard) to monitor task execution, inspect logs, and troubleshoot any issues.

---

#### V.Features
1. **Data Reading**: Read historical Dow Jones data from a Local Temp Parquet file using polars.
2. **Database Integration**: Append the data into a MySQL database hosted in Google Cloud Storage.
3. **Daily Updates**: Ensure the pipeline runs daily to append new data automatically.
4. **Orchestration**: Use Prefect v3.0 to orchestrate and monitor the data pipeline.

---

#### VI. Requirements
- Python 3.8+
- Libraries:
  - yfinance
  - polars
  - prefect (v3.0 or higher)
  - mysql-connector-python
  - google-cloud-storage
- MySQL instance hosted on Google Cloud Storage
- Google Cloud service account with appropriate permissions