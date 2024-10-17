import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="your_host",
    database="your_database",
    user="your_username",
    password="your_password"
)

cursor = conn.cursor()


#PULL INFORMATION ON TABLE
query = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'your_table_name';
"""
cursor.execute(query)
columns = cursor.fetchall()

#Print Schema
for column in columns:
    print(f"Column: {column[0]}, Data Type: {column[1]}, Nullable: {column[2]}")




#Query and Pull
query = "SELECT * FROM your_table"
cursor.execute(query)
rows = cursor.fetchall()

#Convert to Dataframe
df = pd.DataFrame(rows)

# Assuming 'field_name' is the column with nested entries
nested_fields = df['field_name'].apply(lambda x: isinstance(x, list))

#Extract the nested entries into a new DataFrame:
nested_df = df[nested_fields].explode('field_name')
nested_df.to_csv('nested_entries.csv', index=False)

#You can create a new table to store the one-to-many relationship:
nested_df.to_sql('one_to_many_table', conn, if_exists='replace', index=False)
