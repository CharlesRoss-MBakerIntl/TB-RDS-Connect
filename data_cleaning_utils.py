import pandas as pd
from fuzzywuzzy import process




def clean_empty_none(fields, df):
    
    #Create Empty DataFrame
    cleaned_df = pd.DataFrame()

    #Check if Provdied Fields in Dataframe
    for field in fields:
        if field not in df.columns:
            raise Exception(f"Error: {field} Not In DataFrame")


    #Itterate Through DataFrame and Check for Empty Values
    for index, row in df.iterrows():
        
        #Create Add Check for Row
        add_check = True

        #Check for None or Empty Strings in Row Fields
        for field in fields:
            if row[field] == None or row[field] == '':
                add_check = False

        #If Add Check Still True, Add to Cleaned DataFrame
        try:
            if add_check == True:
                cleaned_df = pd.concat([cleaned_df, pd.DataFrame(row).T])

        except Exception as e:
            raise Exception(f'Error: Failed to Add Row to Cleaned DataFrame {e}')


    return cleaned_df




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