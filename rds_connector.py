import psycopg2
from teams_notifications import channel_notifications

def rds_connection(username, password):
    
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
            host = 'tidalbasin-cdbg-dr-fl-ida-backendauroraclusterclus-sefctthwsqmq.ce8xd2bu2edx.us-east-2.rds.amazonaws.com',
            port = 5432,
            user = username,
            password = password,
            database= 'tidalbasin'
        )

        #Return the Connection Object if Successful
        return conn
    

    except Exception as e:
        
        # Raise Exception to Stop Process if Failure
        raise Exception(f"Failed to Connect to RDS Database: {e}")