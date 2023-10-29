import sqlite3
import pandas as pd

db_name = "STAFF.db"
table_name = "INSTRUCTOR"
csv_file = "data-source/INSTRUCTOR.csv"
attribute_list=["ID","FNAME","LNAME","CITY","CCODE"]

df = pd.read_csv(csv_file, names=attribute_list)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists="replace", index=False)


# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Define data to be appended
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)


# Append data to the table
data_append.to_sql(table_name, conn, if_exists = 'append', index = False)
print('Data appended successfully')

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT * FROM {table_name} where ID=100"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()
