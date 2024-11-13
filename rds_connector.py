"""
This Python script is designed to manage connections to an AWS RDS database and handle database queries and data schema validation for Power BI reports. 
It leverages the `psycopg2` library for database connections and `pandas` for data manipulation. The script includes a function to establish a connection 
to the RDS database and a class to handle database queries, validate data schema, and perform data cleaning.

Functions:
    - rds_connection(username, password, db, server): 
      Establishes a connection to an AWS RDS database using the provided credentials.

    - rds_sql_pull(cursor, query): 
      Executes a SQL query using the provided cursor, fetches rows, and converts them to a Pandas DataFrame.

    - build_clean_list(join_list): 
      Builds a list of cleaning steps based on the join list provided. 
      
    - build_schema(source=None, join_list=None, schema=None, exclude=None): 
      Constructs a schema list from the source and join list, optionally excluding specified fields. 
      
    - build_query(source=None, join_list=None, query=None): 
      Constructs a SQL query from the source and join list. 
      
    - clean_empty_none(field, df, removed): 
      Cleans rows with empty or None values from the specified field in the DataFrame. 
      
    - convert_dates(field, df, output_format=None): 
      Converts string dates in the specified field to datetime objects in the DataFrame. 
      
    - convert_integer(field, df): 
      Converts string numbers in the specified field to integers in the DataFrame. 
      
    - update_field_names(self, table_fields, new_fields): 
      Updates column names in the DataFrame based on user-provided lists of current and new field names.

      
Classes:
    - RDS: 
      Handles database queries, data schema validation, and data cleaning.

      
Dependencies:
    - psycopg2
    - pandas
    - codecs
    - datetime

Created by: Charles Ross
Contact: charles.ross@mbakerintl.com
Last updated by: Charles Ross on 11/13/2024
"""



################    IMPORT PACKAGES    ######################

import psycopg2
import pandas as pd
import codecs



################    RDS TABLE CONNECTION    ######################

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
    


 


################    RDA TABLE PULL CLASS    ######################

#----------------------------------------------------------------

class RDS:
    
    """
    A class used to handle database queries, data schema validation, and data cleaning for AWS RDS databases.

    Methods:
    --------
    __init__(conn, cursor, query_package=None, auto=True, schema=None, exclude=None, query=None):
        Initializes the RDS class with database connection, cursor, and query details, 
        and sets up the schema and cleaning lists.

    check_schema(df):
        Validates if the DataFrame matches the expected schema defined in the class.

    clean_table(data):
        Cleans the provided DataFrame based on predefined cleaning steps and maintains a log of the cleaning versions.

    query_to_df(clean=True):
        Executes the stored SQL query, validates the schema, and optionally cleans the resulting DataFrame.
    
    Attributes:
    -----------
    conn : psycopg2.extensions.connection
        Connection to the AWS RDS database.
    cursor : psycopg2.extensions.cursor
        Cursor for executing database queries.
    query_package : dict or None
        Package containing query details, source, and join information.
    schema : list or None
        List of expected columns in the DataFrame.
    clean_list : list
        List of cleaning steps to be applied to the DataFrame.
    df : pd.DataFrame
        DataFrame to hold query results.
    removed : pd.DataFrame
        DataFrame to store rows removed during cleaning.
    cleaning_versions : list
        List to log versions of the DataFrame at each cleaning step.
    fields_missing : list
        List of fields missing from the DataFrame if schema validation fails.

    Explanation:
    ------------
    The `RDS` class provides a structured way to handle database interactions with AWS RDS, including querying 
    and cleaning data to meet specific schema requirements. The class can automatically execute a query and 
    process the data upon initialization or wait until explicitly called to generate the DataFrame. It leverages 
    the `psycopg2` library for database connectivity and `pandas` for data manipulation and validation.
    """


    def __init__(self, conn, cursor, query_package = None, auto = True, schema = None, exclude = None, query = None):
        
        #Store Connector and Query Package 
        self.conn = conn
        self.cursor = cursor
        self.query_package = query_package

        #Unpack the Query
        self.source, self.join_list = unpack_query(query_package)

        #Create Schema, Query, and Clean List
        self.schema = build_schema(source = self.source, join_list = self.join_list, schema = schema, exclude = exclude)
        self.query = build_query(query = query, source = self.source, join_list = self.join_list)
        self.clean_list = build_clean_list(join_list = self.join_list)
     
        #Store Empty Variables for Later Use
        self.removed = pd.DataFrame()
        self.cleaning_versions = []
        self.fields_missing = []

        #If Automatically Generate Data is set to True, Generate Data
        if auto == True:
            self.df = self.query_to_df()

        #If Automatically Generate Data is set to False, Let DF be blank till query is called
        elif auto == False:
            self.df = pd.DataFrame()

        
        


    def check_schema(self, df):

        """
        Validates if the DataFrame matches the expected schema defined in the class.

        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame to be checked against the schema.

        Returns:
        --------
        bool
            True if the DataFrame matches the schema, False otherwise.

        Raises:
        -------
        Exception
            If the DataFrame is empty.

        Explanation:
        ------------
        This method checks if the DataFrame (`df`) contains all the fields specified in the class attribute `self.schema`. 
        If any fields are missing, it sets the check to False and logs the missing fields in `self.fields_missing`. 
        If the DataFrame is empty, an exception is raised. The function ensures that the DataFrame structure aligns with the expected schema.

        Note:
        -----
        Any discrepancy between the DataFrame and the schema, or an empty DataFrame, will result in an exception.
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
    



    def clean_table(self, data):
        
        """
        Cleans the provided DataFrame based on predefined cleaning steps and maintains a log of the cleaning versions.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame to be cleaned.

        Returns:
        --------
        pd.DataFrame
            The cleaned DataFrame.

        Explanation:
        ------------
        This method performs a series of cleaning operations on the provided DataFrame. For each step, it applies the corresponding 
        cleaning function, such as removing empty or None values, converting date strings to datetime objects, and converting 
        string numbers to integers. The cleaned data at each step is logged in self.cleaning_versions for submittal to 
        the archive. The final cleaned DataFrame is returned.

        Note:
        -----
        - The `self.cleaning_versions` list logs the state of the DataFrame at each cleaning step.
        - The `self.removed` DataFrame stores rows that are removed during the cleaning process.
        """

        #Reset Cleaning Versions 
        if len(self.cleaning_versions) != 0:
            self.cleaning_versions = []

        #Reset Cleaning Versions
        if self.removed.empty == False:
            self.removed = pd.DataFrame()
        
        #Add Original Data to Clean Version List
        self.cleaning_versions.append({"Step": 'Original DataFrame', "Field": None, "Result": data})

        for step in self.clean_list:
            
            #Store Field
            field = step['field']  # Adjust based on your actual structure
            
            #Store Function
            clean_function = step['function']    # Assuming your steps include the type of calculation
            
            #Perform Cleaning
            if clean_function:
                
                #Clean Emtpy Nones from Data, Save Removals
                if clean_function == clean_empty_none:
                    version = "Clean Nulls and Empty Fields"
                    data, self.removed = clean_function(field, data, self.removed)

                #Clean Emtpy Nones from Data, Save Removals
                elif clean_function == convert_dates:
                    version = "Convert String Dates to DateTimes"
                    data = clean_function(field, data)

                #Clean Emtpy Nones from Data, Save Removals
                elif clean_function == convert_integer:
                    version = "Convert String Numbers to Integers"
                    data = clean_function(field, data)

                #CONTINUE ADDING FUNCTIONS


                #Add Version and Step to Clean Version List    
                self.cleaning_versions.append({"Step": version, "Field": field, "Result": data})
            

        #Add Final DataFrame
        self.cleaning_versions.append({"Step": 'Final DataFrame', "Field": None, "Result": data})

        #Return Cleaned Data
        return data
    



    def query_to_df(self, clean = True):
        
        """
        Executes the stored SQL query, validates the schema, and optionally cleans the resulting DataFrame.

        Parameters:
        -----------
        clean : bool, optional
            If True, performs cleaning operations on the DataFrame. Default is True.

        Returns:
        --------
        pd.DataFrame
            The resulting DataFrame from the SQL query after schema validation and optional cleaning.

        Raises:
        -------
        Exception
            If the SQL query execution fails, the DataFrame schema does not match the expected schema, 
            or if the resulting DataFrame is empty.

        Explanation:
        ------------
        This method executes the stored SQL query (`self.query`) and validates the resulting DataFrame against 
        the expected schema (`self.schema`). It drops duplicate rows and updates the class attribute DataFrame (`self.df`). 
        If the `clean` parameter is set to True, it also applies cleaning operations to the DataFrame. 
        If the DataFrame schema does not match, it raises an exception and lists the missing fields.

        Note:
        -----
        - Ensure that `self.query` and `self.schema` are set before calling this method.
        - Any mismatch in the DataFrame schema or an empty result from the SQL query will raise an exception.
        """
        
        
        if (self.query != None) & (self.schema != None):

            #Update DataFrame with SQL Query
            data = rds_sql_pull(self.cursor, self.query)

            #Check if Data Empty
            if data.empty == False:
                #Check Schema
                if self.check_schema(data):

                    #Drop Duplicate Data
                    data = data.drop_duplicates()
                    
                    #Update DF with Data
                    self.df = data

                    #If Selected, Update Columns
                    if clean == True:
                        self.df = self.clean_table(data)


                    #Return DataFrame
                    return self.df
                
                else:
                    print(f'Missing Fields from Table:  {self.fields_missing}')
                    raise Exception("Error: DataFrame Schema Did Not Match, Check fields_missing()")
                
            else:
                raise Exception("Error: DataFrame Emtpy from SQL Query")
            
        else:
            raise Exception("Error: DataFrame Emtpy from SQL Query")

        






################    RDS PROCESSING FUNCTIONS    ######################

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

def unpack_query(query_package):

    """
    Unpacks the query package to extract the source and join list components.

    Parameters:
    -----------
    query_package : dict
        A dictionary containing the query details including source and join list components.

    Returns:
    --------
    tuple
        A tuple containing the source component and the join list component.

    Raises:
    -------
    Exception
        If the query package exists but does not contain the source or join list components.

    Explanation:
    ------------
    This function checks if the `query_package` is not None and then attempts to extract the 'source' and 'join_list' components from it. 
    If either component is missing, an exception is raised indicating the specific missing component. If the `query_package` is None, 
    both the source and join list are set to None. The function ensures that the necessary components are present for further processing 
    and returns them as a tuple.

    Note:
    -----
    Any discrepancy in the `query_package`, such as missing source or join list, will raise an exception.
    """

    #If Join List NOT Empty
    if query_package != None:
        
        if 'source' in query_package:
            source = query_package["source"]
        else:
            source = None
            raise Exception("Query Package Exists, but Not Source Component Identified")
        
        if 'join_list' in query_package:
            join_list = query_package["join_list"]
        else:
            source = None
            raise Exception("Query Package Exists, but Not Join List Component Identified")    
                           
    #Join List Empty           
    else:
        source = None
        join_list = None     


    #Return Clean List
    return source, join_list






#----------------------------------------------------------------

def build_clean_list(join_list):

    """
    Builds a list of cleaning steps based on the join list provided.

    Parameters:
    -----------
    join_list : list
        A list of dictionaries containing field names and their associated cleaning operations.

    Returns:
    --------
    list
        A list of dictionaries where each dictionary contains a field and its corresponding cleaning function.

    Raises:
    -------
    Exception
        If a specified cleaning operation does not match any function in the cleaning dictionary.

    Explanation:
    ------------
    This function creates a cleaning dictionary that maps specific cleaning operations to their respective functions. 
    It then iterates through the provided join list, extracting the fields and their cleaning operations. 
    For each operation, it attempts to match it with a function from the cleaning dictionary and adds it to the clean list. 
    If an operation does not match any function in the dictionary, an exception is raised. 
    The function ensures that all specified cleaning operations are mapped to their respective functions and returns the clean list.

    Note:
    -----
    If the join list is empty, the function returns an empty clean list.
    """

    #If Join List NOT Empty
    if join_list != None:

        #Create Clean Dictionary
        clean_dict = {
        'NULL' : clean_empty_none,
        'DATE_CONVERT': convert_dates,
        'INT_CONVERT': convert_integer
        }

        #Create Clean List
        clean_list = []

        # Cycle Through Clean Lists
        for item in join_list:
            for field in item['clean']:
                for name, calcs in field.items():
                    for calc in calcs:
                        
                        #Attempt to Pull the Function Step from the Dictionary
                        try:

                            #If Calc in Dictionary
                            if (clean_dict[calc] != None):
                                
                                #Create Entry from Dictionary
                                entry = {'field': name,
                                        'function': clean_dict[calc]}

                                #Add to Clean List
                                clean_list.append(entry)

                    
                            #If Calc not in Dictionary
                            elif (clean_dict[calc] == None):
                                raise Exception(f'Error: {calc} did not match any function in the function dictionary.')

                        except Exception as e:
                            raise Exception(f"Error: Could not add calc entry to clean list.  Traceback:{e}")  

    #Join List Empty           
    else:
        clean_list = []        


    #Return Clean List
    return clean_list



#----------------------------------------------------------------

def build_schema(source = None, join_list = None, schema = None, exclude = None):
    
    """
    Constructs a schema list from the source and join list, optionally excluding specified fields.

    Parameters:
    -----------
    source : dict, optional
        A dictionary containing the source table and its fields.
    join_list : list, optional
        A list of dictionaries containing the join tables and their fields.
    schema : list, optional
        A list of expected columns in the DataFrame.
    exclude : list, optional
        A list of fields to be excluded from the schema.

    Returns:
    --------
    list
        A list of fields representing the constructed schema.

    Raises:
    -------
    Exception
        If there is an error building the schema from the source or join list.

    Explanation:
    ------------
    This function creates an empty schema list and populates it with fields from the source and join list if provided. 
    If the schema is manually passed into the function, it uses that schema. It also filters out any fields specified 
    in the exclude list. If any error occurs during the construction of the schema from the source or join list, 
    an exception is raised. The function ensures that the resulting schema list contains the required fields for further processing.

    Note:
    -----
    Any discrepancy in the source or join list data packages will raise an exception.
    """

    # Create Empty Schema List
    schema_lst = []

    #If Source and Join List Data Present
    if (schema == None) & (source != None) & (join_list != None):
        
        try:
            #Add Fields from Source
            for field in source['fields']:
                for tn, jn in field.items():
                    schema_lst.append(jn)


            # Add Fields from Joins
            for item in join_list:
                for field in item['fields']:
                    for tn, jn in field.items():
                            schema_lst.append(jn)
    
        except Exception as e:
            raise Exception(f"Error: Could not build schema from source or join list, check data packages.  Traceback: {e}")


    #If Source Manually Passed Into Function
    elif (schema != None) & (source == None) & (join_list == None):
        schema_lst = schema

    #Filter Schema List
    if exclude != None:
        schema_lst = [field for field in schema_lst if field not in exclude]

    #Return Schema List
    return schema_lst


#----------------------------------------------------------------

def build_query(source = None, join_list = None, query = None):

    """
    Constructs a SQL query from the source and join list or uses a provided query.

    Parameters:
    -----------
    source : dict, optional
        A dictionary containing the source table and its fields.
    join_list : list, optional
        A list of dictionaries containing the join tables and their fields.
    query : str, optional
        An existing SQL query string to be used.

    Returns:
    --------
    str
        The constructed SQL query string.

    Raises:
    -------
    Exception
        If there is an error pulling fields from the source or join list data packages, or applying project filters and order operations.

    Explanation:
    ------------
    This function constructs a SQL query by first initializing the query string with a SELECT statement. It adds fields from 
    the source table and join tables, includes JOIN clauses for the join tables, and applies a WHERE clause to filter by project 
    and an ORDER BY clause to sort the results. If the source and join list are not provided, it returns the existing query 
    passed as an argument. If there is any error during the construction of the query, an exception is raised. The function 
    ensures that the resulting query string is correctly formatted and ready for execution.

    Note:
    -----
    Any discrepancy in the source or join list data packages, or failure to apply filters and order operations, will raise an exception.
    """
    
    #If Source and Join List Passed, Query 
    if (query == None) & (source != None) & (join_list != None):
        
        #Set Count for Join Sections
        join_count = 1

        #Initialize Query Select
        query = "SELECT\n"

        #Add Fields from Source
        try:
            for field in source['fields']:
                for tn, jn in field.items():
                    query += f"    {source['name']}.{tn} AS {jn},\n"

        except Exception as e:
            raise Exception(f"Error: Could not pull fields from Source, check Source data package.  Traceback: {e}")



        # Add Fields from Joins
        try:
            for index, item in enumerate(join_list):
                # Add Join Segments
                for field in item['fields']:
                    for tn, jn in field.items():
                        if index == len(join_list) - 1 and field == item['fields'][-1]:
                            query += f"    {item['name']}.{tn} AS {jn}\n"
                        else:
                            query += f"    {item['name']}.{tn} AS {jn},\n"

        except Exception as e:
            raise Exception(f"Error: Could not pull fields from Join List, check Join List data package.  Traceback: {e}")



        #Add Source
        try:
            query += "\nFROM\n"
            query += f"    {source['table']} {source['name']}\n"
        
        except Exception as e:
            raise Exception(f"Error: Could not pull source table or name from Source data package.  Traceback: {e}")



        #Add Joins
        try:
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

        except Exception as e:
            raise Exception(f"Error: Could not pull join information from join list, check join list data package.  Traceback: {e}")



        #Filter Project
        try:
            query += "\n WHERE \n"
            query += f"    {source['name']}.project_id = {source['project']}"

        except:
            raise Exception(f"Error: Could not apply project filter to query.  Traceback: {e}")



        #Order Project
        try:
            query += "\n ORDER BY\n"
            query += f"{source['name']}.{source['order']};"
        
        except:
            raise Exception(f"Error: Could not apply order operations to query.  Traceback: {e}")
        
            
        #Encode String
        query = codecs.decode(query.encode(), 'unicode_escape')



    # If Query Passed, Pass Back Query
    elif (query != None) & (source == None) & (join_list == None):
        query = query


    #Return Query
    return query










################    DATA CLEANING FUNCTIONS    ######################

#----------------------------------------------------------------

def clean_empty_none(field, df, removed):

    """
    Cleans rows with empty or None values from the specified field in the DataFrame and maintains a log of the removed rows.

    Parameters:
    -----------
    field : str
        The name of the field to be checked for empty or None values.
    df : pd.DataFrame
        The DataFrame to be cleaned.
    removed : pd.DataFrame
        A DataFrame to store rows that are removed during the cleaning process.

    Returns:
    --------
    tuple
        A tuple containing the cleaned DataFrame and the DataFrame of removed rows.

    Raises:
    -------
    Exception
        If the specified field is not in the DataFrame or if there is an error adding rows to the cleaned DataFrame.

    Explanation:
    ------------
    This function iterates through the DataFrame, checking for empty or None values in the specified field. 
    Rows with empty or None values are added to the `removed` DataFrame, while other rows are added to the `cleaned_df` DataFrame. 
    If the specified field is not found in the DataFrame, an exception is raised. The function ensures that the DataFrame is cleaned 
    by removing rows with empty or None values in the specified field and returns the cleaned DataFrame along with the removed rows.

    Note:
    -----
    Any discrepancy in the field names or errors during the row addition process will raise an exception.
    """
    
    #Create Empty DataFrame
    cleaned_df = pd.DataFrame()

    #Check if Field in Dataframe
    if field not in df.columns:
        raise Exception(f"Error: {field} Not In DataFrame")


    #Itterate Through DataFrame and Check for Empty Values
    for index, row in df.iterrows():
        
        #Create Add Check for Row
        add_check = True

        #Check for None or Empty Strings in Row Fields
        if row[field] == None or row[field] == '':
            add_check = False

        #If Add Check Still True, Add to Cleaned DataFrame
        try:
            if add_check == True:
                cleaned_df = pd.concat([cleaned_df, pd.DataFrame(row).T])

            elif add_check == False:
                removed = pd.concat([removed, pd.DataFrame(row).T])

        except Exception as e:
            raise Exception(f'Error: Failed to Add Row to Cleaned DataFrame {e}')

        
    return cleaned_df, removed




#----------------------------------------------------------------

def convert_dates(field, df, output_format = None):

    """
    Converts string dates in the specified field to datetime objects in the DataFrame.

    Parameters:
    -----------
    field : str
        The name of the field containing date strings to be converted.
    df : pd.DataFrame
        The DataFrame containing the field to be converted.
    output_format : str, optional
        The format to which the datetime objects should be converted. If not provided, the default datetime format is used.

    Returns:
    --------
    pd.DataFrame
        The DataFrame with the converted date field.

    Raises:
    -------
    Exception
        If the specified field is not in the DataFrame or if there is an error converting the field to datetime.

    Explanation:
    ------------
    This function checks if the specified field is present in the DataFrame. It then converts the string dates in the field to datetime objects 
    using `pandas.to_datetime()`. If an output format is provided, the datetime objects are converted to the specified string format. 
    If there is any error during the conversion process, an exception is raised. The function ensures that the date strings in the specified field 
    are correctly converted to datetime objects or formatted strings and returns the updated DataFrame.

    Note:
    -----
    Any discrepancy in the field names or errors during the conversion process will raise an exception.
    """

    #Check if Field in DataFrame
    if field not in df.columns:
        raise Exception(f"Error: {field} Not In DataFrame")

    
    try:
        df[field] = pd.to_datetime(df[field])
        
        #Convert to String Format if output_format passed
        if output_format != None:
            df[field] = df[field].dt.strftime(output_format)
            
        return df
    
    except Exception as e:
       raise Exception(f"Error: Could not convert {field} to datetime:  Traceback{e}")
    



#----------------------------------------------------------------

def convert_integer(field, df):

    """
    Converts string numbers in the specified field to integers in the DataFrame.

    Parameters:
    -----------
    field : str
        The name of the field containing string numbers to be converted.
    df : pd.DataFrame
        The DataFrame containing the field to be converted.

    Returns:
    --------
    pd.DataFrame
        The DataFrame with the converted integer field.

    Raises:
    -------
    Exception
        If the specified field is not in the DataFrame or if there is an error converting the field to integers.

    Explanation:
    ------------
    This function checks if the specified field is present in the DataFrame. It then converts the string numbers in the field to integers 
    using `pandas.to_numeric()` with error coercion. If there is any error during the conversion process, an exception is raised. 
    The function ensures that the string numbers in the specified field are correctly converted to numeric values and returns the updated DataFrame.

    Note:
    -----
    Any discrepancy in the field names or errors during the conversion process will raise an exception.
    """

    #Check if Field in DataFrame
    if field not in df.columns:
        raise Exception(f"Error: {field} Not In DataFrame")
    
    try:
        df[field] = pd.to_numeric(df[field], errors='coerce')
        return df
    
    except Exception as e:
        raise Exception(f"Error: Could not convert {field} to integer:  Traceback{e}")




#----------------------------------------------------------------

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