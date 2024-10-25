import psycopg2
import pandas as pd
import codecs


#----------------------------------------------------------------

def rds_connection(username, password, db, server):
    
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
    



#----------------------------------------------------------------

def sql_query(source, join_list):
    
    #Set Count for Join Sections
    join_count = 1

    #Initialize Query Select
    query = "SELECT\n"

    #Add Fields from Source
    for field in source['fields']:
        for tn, jn in field.items():
            query += f"    {source['name']}.{tn} AS {jn},\n"


    # Add Fields from Joins
    for index, item in enumerate(join_list):
        # Add Join Segments
        for field in item['fields']:
            for tn, jn in field.items():
                if index == len(join_list) - 1 and field == item['fields'][-1]:
                    query += f"    {item['name']}.{tn} AS {jn}\n"
                else:
                    query += f"    {item['name']}.{tn} AS {jn},\n"
                
                
                
                


    #Add Source
    query += "\nFROM\n"
    query += f"    {source['table']} {source['name']}\n"


    #Add Joins
    for index, item in enumerate(join_list):
        
        if index == 0:
            data_conn = 'initial_join_answers'
        else:
            data_conn = f'join_answers_{join_count}'
            join_count += 1

        if item['question_source'] == "JOIN_SOURCE":
            question_source = source['name']

        elif item['question_source'] == "DATA_SOURCE":
            question_source = 'initial_join_answers'
        
        query += f"\nLEFT JOIN application_data_answer {data_conn} ON {question_source}.{item['source_id']} = {data_conn}.{item['join_id']} AND {data_conn}.question_id = {item['question_id']}"
        query += f"\nLEFT JOIN {item['data_source']} {item['name']} ON {data_conn}.id = {item['name']}.answer_ptr_id\n"


    #Filter Project
    query += "\n WHERE \n"
    query += f"    {source['name']}.project_id = {source['project']}"

    #Order Project
    query += "\n ORDER BY\n"
    query += f"{source['name']}.{source['order']};"
    
        
    #Encode String
    query = codecs.decode(query.encode(), 'unicode_escape')


    return query



#----------------------------------------------------------------

def list_tables(cursor):
    
    # Execute the query to get all table names
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)

    # Fetch all table names
    tables = cursor.fetchall()

    # Create Empty List
    table_lst = []

    # Print the table names
    for table in tables:
        table_lst.append(table[0])

    return table_lst



    

#----------------------------------------------------------------

def build_schema(source = None, join_list = None, schema = None, exclude = None):

    # Create Empty Schema List
    schema_lst = []

    #If Source and Join List Data Present
    if (schema == None) & (source != None) & (join_list != None):
    
        #Add Fields from Source
        for field in source['fields']:
            for tn, jn in field.items():
                schema_lst.append(jn)


        # Add Fields from Joins
        for item in join_list:
            for field in item['fields']:
                for tn, jn in field.items():
                        schema_lst.append(jn)

    #If Source Manually Passed Into Function
    elif (schema != None) & (source == None) & (join_list == None):
        schema_lst = schema

    #Filter Schema List
    if exclude != None:
        schema_lst = [field for field in schema_lst if field not in exclude]


    return schema_lst




#----------------------------------------------------------------

def rds_sql_pull(cursor, query):

    """
    Executes a SQL query using the provided cursor, fetches rows, and converts them to a Pandas DataFrame.

    Parameters:
    - cursor: Cursor object for database connection.
    - query: SQL query string to be executed.

    Explanation:
    This function attempts to execute a SQL query using the given cursor. It fetches all rows from the query result 
    and then converts these rows into a Pandas DataFrame. If the conversion to a DataFrame fails, an exception is raised, 
    and the process is stopped to indicate the failure. The function ensures that the SQL query is executed 
    and its results are correctly transformed into a DataFrame format for further data manipulation or analysis.

    Return:
    The DataFrame containing the query results if successful.

    Note:
    Any failure in executing the query or converting the result to a DataFrame triggers an exception, 
    alerting the user to address the issue promptly.
    """

    try:

        # Execute Query
        cursor.execute(query)
        # Fetch Columns
        columns = [col[0] for col in cursor.description]
        # Fetch Rows
        rows = cursor.fetchall()


        try:
        
            #Convert to Dataframe
            df = pd.DataFrame(rows, columns=columns)
            return df


        except Exception as e:
            
            # Raise Exception to Stop Process if Failure
            raise Exception(f"Rows Pulled from Cursor, Failed to Convert to Pandas Dataframe: {e}")


    except Exception as e:

        # Raise Exception to Stop Process if Failure
        raise Exception(f"Failed to Pull Rows from Cursor Query Execute: {e}")





#----------------------------------------------------------------

class RDSTablePull:
    
    """
    RDSTablePull is a class to handle database queries and data schema validation.
    
    Attributes:
    -----------
    conn : connection object
        Connection to the database.
    cursor : cursor object
        Cursor for executing database queries.
    query : str
        SQL query to be executed.
    schema : list
        List of expected columns in the dataframe.
    df : pandas.DataFrame
        DataFrame to hold query results.
    fields_missing : list
        List of missing fields if schema does not match.

    Methods:
    --------
    check_schema(df):
        Validates if the dataframe matches the user provided schema.

    update_field_names(table_fields, new_fields):
        Updates the column names in the dataframe with user provided field names.

    query_to_df(update_col=False, table_fields=None, new_fields=None):
        Executes the SQL query and updates the dataframe with the results, checks 
        schema and updates fields if selected.
    """


    def __init__(self, conn, cursor, query = None, source = None, join_list = None, schema = None, exclude = None):
        self.conn = conn
        self.cursor = cursor
        self.query = query
        self.source = source
        self.join_list = join_list
        self.schema = build_schema(source, join_list, schema, exclude)
        self.clean_list = []
        self.df = pd.DataFrame()
        self.fields_missing = []
        


    def check_schema(self, df):

        """
        Check if the dataframe matches the user provided schema passed to the class. This schema 
        is a list of field names that the user needs to ensure is in the resulting dataframe pull
        from the SQL Database.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Dataframe to check.
        
        Returns:
        --------
        bool
            True if schema matches, else False.
        
        Raises:
        -------
        Exception
            If dataframe is empty.
        """

        #Set Check
        check = True
        
        #Check if DF Empty
        if df.empty == False:
        
            #Check if Fields Exist in DF
            for field in self.schema:
                if field not in df.columns:
                    
                    #Field not Found, Set Check to False and Add Missing Field to List
                    check = False
                    self.fields_missing.append(field)

        else:
            raise Exception("Error: Dataframe Empty When Checking Schema")

        return check
    

    def build_query(self):
        
        #If Query None
        if self.query == None:
            try:
                query = sql_query(self.source, self.join_list)
                return query 
            
            except Exception as e:
                raise Exception(f"Error: Could not build query from source and join list. {e}")

        #Return Query if Present
        elif self.query != None:
            return self.query
    

    def clean_table(self):
        pass


    def update_field_names(self, table_fields, new_fields):

        """
        Update column names in the dataframe.  User provides list of fields that need to be updated in the
        table and a list of fields names that they need to be updated to.  The fields must match in each list.
        
        Parameters:
        -----------
        table_fields : list
            Current column names.
        new_fields : list
            New column names.
        
        Raises:
        -------
        Exception
            If dataframe is empty or rename fails.
        """
        
        #Create Dictionary for Fields Update
        field_dict = dict(zip(table_fields, new_fields))

        #Update Fields
        for table_field, new_field in field_dict.items():

            if self.df.empty == False :
            
                try:
                    #Update the Column Name
                    self.df.rename(columns={table_field: new_field}, inplace=True)

                except Exception as e:
                    raise Exception(f"Failure to Convert {table_field} to {new_field}")
    
            else:
                raise Exception("Error: Cannot Rename Columns of Empty Dataframe")



    def query_to_df(self, update_col = False, table_fields = None, new_fields = None):
        
        """
        Execute the SQL query and update the dataframe with the results, optionally renaming columns.
        
        Parameters:
        -----------
        update_col : bool, optional
            Whether to update column names based on provided lists (default is False).
        table_fields : list of str, optional
            Current column names in the dataframe. Required if update_col is True.
        new_fields : list of str, optional
            New column names to update to. Required if update_col is True.

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing the results of the executed SQL query.

        Raises:
        -------
        Exception
            If the dataframe is empty after executing the query.
            If the dataframe schema does not match the expected schema.
            If there is an error updating column names when update_col is True.
        
        Notes:
        ------
        This method pulls data from an RDS instance using the specified SQL query,
        checks the resulting dataframe against the provided schema, and optionally 
        updates the column names. It ensures that the dataframe is neither empty nor 
        incorrectly structured before returning the updated dataframe.
        """
        
        #Build Query 
        query = self.build_query()

        #Update DataFrame with SQL Query
        data = rds_sql_pull(self.cursor, query)

        #Check if Data Empty
        if data.empty == False:
            #Check Schema
            if self.check_schema(data):
                
                #Update DF with Data
                self.df = data

                #If Selected, Update Columns
                if update_col == True:
                    self.update_field_names(table_fields, new_fields)

                #Return DataFrame
                return self.df
            
            else:
                print(f'Missing Fields from Table:  {self.fields_missing}')
                raise Exception("Error: DataFrame Schema Did Not Match, Check fields_missing()")
            
        else:
            raise Exception("Error: DataFrame Emtpy from SQL Query")

        

    
