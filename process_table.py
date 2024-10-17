# ----------  LOAD PACKAGES AND FUNCTIONS  -----------
import os

from rds_connector import rds_connection
from teams_notifications import channel_notification




# ------------  LOAD SECRET VARIABLE  -------------
username =  os.environ("USER")
password = os.environ("PASS")
db = os.environ("DB")
server = os.environ("SERVER")





# ----------  CONNECT TO AWS RDS DATA TABLE  -----------

try:

    #Connect to AWS RDS Database
    conn = rds_connection(username,
                          password,
                          db,
                          server)
    
except Exception as e:

    #Notify Teams of Failure
    channel_notification(alert_type = "Error", 
                         summary = "RDS CONNECTION ERROR", 
                         message = "Could not connect to the AWS RDS Database",
                         error = e)
    
    #Raise Error
    raise(f"AWS RDS CONNECTION ERROR: {e}")






# ---------------  SCHEMA ANALYSIS  ---------------