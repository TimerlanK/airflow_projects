# airflow user_processing project
**dags/user_processing.py**
</br>The project showcases a complete data pipeline flow, executing SQL queries, running Python functions, interfacing with APIs, using conditional sensors, leveraging hooks for database operations, and facilitating task communication through XComs.

</br>![image](https://github.com/TimerlanK/airflow_user_processing_project/assets/59342509/b081886e-9a52-4884-919b-14834bb8d44f)
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





# airflow webparser__to_greenplum project
