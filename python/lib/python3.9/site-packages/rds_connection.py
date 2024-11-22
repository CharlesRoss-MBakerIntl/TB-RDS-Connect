""" 
This Python script is engineered to establish connections to an AWS RDS PostgreSQL database using provided credentials. 
It includes utility functions to manage these connections efficiently and handle potential failures by notifying a Teams channel. 
The script utilizes the `psycopg2` library for database connectivity, ensuring reliable and secure access to the database.

Functions: 
    - rds_connection(username, password, db, server): 
      Establishes a connection to an AWS RDS database using the provided credentials and notifies a Teams channel in case of connection failure.

      
Dependencies:

    - psycopg2

    
Created by: Charles Ross
Contact: charles.ross@mbakerintl.com
Last updated by: Charles Ross on 11/19/2024.

"""


################    IMPORT PACKAGES    ######################
import psycopg2


################    RDS TABLE CONNECTION    ######################

#----------------------------------------------------------------

def connect_rds(username, password, db, server):
    
    """
    Establishes a connection to an AWS RDS database using the provided credentials and notifies a Teams channel in case of connection failure.

    Parameters:
    - username: Username for database connection.
    - password: Password for the database connection.

    Explanation:
    This function attempts to connect to an AWS RDS database using the given credentials. It uses the psycopg2 library to establish the connection 
    and returns the connection object if successful. If the connection attempt fails, an exception is raised, and the Teams channel is 
    notified about the RDS connection failure by sending an error alert with details. The process is stopped by raising an exception, 
    indicating the failure to connect to the RDS database.

    Return:
    The connection object if the connection is successful (None otherwise).

    Note:
    The Team's channel notification is triggered when there is a connection failure, providing an immediate alert to relevant 
    team members to address the issue promptly.
    """

    try:

        #Connect to the AWS RDS Database
        conn = psycopg2.connect(
            host = server,
            port = 5432,
            user = username,
            password = password,
            database= db
        )

        cursor = conn.cursor()

        #Return the Connection Object if Successful
        return conn, cursor
    

    except Exception as e:
        
        # Raise Exception to Stop Process if Failure
        raise Exception(f"Failed to Connect to RDS Database: {e}")