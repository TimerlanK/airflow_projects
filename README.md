# user_processing project
**dags/user_processing.py**
</br>The project showcases a complete data pipeline flow, executing SQL queries, running Python functions, interfacing with APIs, using conditional sensors, leveraging hooks for database operations, and facilitating task communication through XComs.

**Results**
</br>![image](https://github.com/TimerlanK/airflow_projects/assets/59342509/97bb2e78-ee21-4585-ad49-f4dd47da8b5e)
</br>![image](https://github.com/TimerlanK/airflow_projects/assets/59342509/122604e5-061d-4bc8-bfa8-570f6fd9fb37)
</br><img src="https://github.com/TimerlanK/airflow_projects/assets/59342509/58a2ddb2-d149-4dcd-a8af-ef681a79cdaf" width="700" height="250">

**List of connections**
</br>![image](https://github.com/TimerlanK/airflow_user_processing_project/assets/59342509/9e8ed32b-75a8-4238-9129-93e3506aaad9)

**SQL Execution with PostgresOperator**
</br>The create_table task demonstrates the use of PostgresOperator to run SQL statements, particularly creating a users table in the database, showcasing the integration of Airflow with PostgreSQL for database management.

**Python Function Execution with PythonOperator** 
</br>Utilizing the PythonOperator, the process_user and store_user tasks execute predefined Python functions. These functions are central to processing the extracted data and storing it into the database, illustrating Airflow's capacity to run arbitrary Python code seamlessly.

**HTTP Request Execution with SimpleHttpOperator**
</br>The extract_user task employs SimpleHttpOperator to issue HTTP GET requests, retrieving user data from a specified API endpoint. This highlights Airflow's ability to interact with external HTTP services directly from a workflow.

**Sensors for Conditional Waiting**
</br>The is_api_available task uses an HttpSensor to pause the workflow until an API becomes accessible, underlining the utility of sensors in Airflow to introduce conditional waiting based on external factors.

**Hook Usage for Database Interaction**
</br>By using PostgresHook within the _store_user function, the workflow interacts with the PostgreSQL database to execute advanced operations like bulk data copying. This exemplifies how hooks in Airflow provide interfaces to external systems.

**Data Exchange Between Tasks**
</br>The pipeline features data exchange between tasks, with process_user retrieving data from extract_user via ti.xcom_pull. This exchange is further extended as processed data is saved and then picked up by another task for database storage, illustrating the XCom feature for tasks communication within Airflow.





# webparser_to_greenplum project
**dags/parser_cbr_v2y.py**
</br>The project automates the daily extraction of currency exchange rate data from the Central Bank of Russia, transforms it into a CSV format, and loads it into a Greenplum database using Apache Airflow.

**Results**
</br>![image](https://github.com/TimerlanK/airflow_projects/assets/59342509/84f38b07-dd51-499f-8d9a-a9f6eb3657b5)
</br><img src="https://github.com/TimerlanK/airflow_projects/assets/59342509/1949cc01-ad33-46b0-8786-7f163f23cffc" width="700" height="250">


**Dynamic URL Generation**
</br>A Python function get_cbr_url dynamically generates the URL to fetch the XML data from the Central Bank of Russia's website, using the execution date of the DAG to retrieve the relevant day's exchange rates.

**BashOperator for Data Retrieval and Preprocessing**
</br>The export_cbr_xml task uses a BashOperator to check if an XML file already exists in the temporary directory and deletes it if present. It then downloads the XML data, converting it from Windows-1251 encoding to UTF-8, and saves it to /tmp.

**XML to CSV Conversion**
</br>The xml_to_csv task is a PythonOperator that reads the XML file, parses it, and converts the data into CSV format. It extracts relevant information such as currency codes, nominal values, and exchange rates for each currency, and logs this data for debugging purposes.

**CSV Loading to Greenplum**
</br>The load_csv_to_gp task also uses a PythonOperator to load the converted CSV data into the Greenplum database. It utilizes a PostgresHook configured with a Greenplum connection to execute a COPY command, efficiently loading the data into the specified table.


# ricky and morty API
**dags/ram_location.py**

**Results**
</br>![image](https://github.com/TimerlanK/airflow_projects/assets/59342509/7463b233-9776-4c1d-9a93-d2bf38812eeb)

**Create a PostgreSQL Table in GreenPlum**
</br>A table named t_kajyrmagambetov_ram_location is created if it does not already exist. This table is meant to store data fetched from an API, with columns for id, name, type, dimension, and resident count.

**Fetch Data from the Rick and Morty API**
</br>The script makes a GET request to the Rick and Morty API to retrieve information about different locations within the show's universe. It then processes this data to find the top 3 locations with the highest resident count. These top 3 locations are then serialized into JSON format to be passed along to the next task in the workflow.

**Insert Data into the PostgreSQL Table**
</br>This task takes the top 3 locations data from the previous task and inserts it into the t_kajyrmagambetov_ram_location table in the GreenPlum database. The connection to the database is managed by Airflow's PostgresHook.
