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


    def __init__(self, conn, cursor, query_package = None, schema = None, exclude = None, query = None, ):
        
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
        self.df = pd.DataFrame()
        self.removed = pd.DataFrame()
        self.cleaning_versions = []
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
    



    def clean_table(self, data):

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
        
        if (self.query != None) & (self.schema != None):

            #Update DataFrame with SQL Query
            data = rds_sql_pull(self.cursor, self.query)

            #Check if Data Empty
            if data.empty == False:
                #Check Schema
                if self.check_schema(data):
                    
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