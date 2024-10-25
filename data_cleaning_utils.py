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