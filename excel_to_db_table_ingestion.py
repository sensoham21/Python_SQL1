
import pandas as pd
import pyodbc
import os
from sqlalchemy import create_engine, MetaData, Table, select





# Getting dataframes, their file names and creating a database with the folder name
def get_dataframes(path,server_name):
    os.chdir(path)
    file_names=[]
    final_df_list=[]
    folder_names = path.split("/")
    folder_name = folder_names[-1]
    ServerName = str(server_name)
    #DbName = str(db_name)
    dbConn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      f'Server={ServerName};'
                      "Trusted_Connection=yes;")
    dbConn.autocommit = True
    dbCursor = dbConn.cursor()
    dbCursor.execute("CREATE DATABASE {}".format(folder_name))
    dbCursor.commit()
    dbConn.close()
    for p,n,f in os.walk(os.getcwd()):
        for a in f:
            a = str(a)
            if a.endswith('.csv'):
                file_names.append(a)
                final_df = a+"_df"
                #print("Dataframe name : "+final_df)
                filename = a
                final_df = pd.read_csv(filename, error_bad_lines=False)
                final_df_list.append(final_df)
    #final_df_list.split(',')
    return final_df_list, file_names, folder_name

# %%
a,b, c = get_dataframes('C:/Users/senso/Downloads/test_folder4','MSI\SQLEXPRESS')

# %%
# Merging the filename and dataframe into a tuple
def unpack_dfs_to_dict(a,b):
    res = {} 
    for key in b:
        key=key[:-4]
        for value in a:
            res[key] = value 
            a.remove(value) 
            break
    return res


x = unpack_dfs_to_dict(a,b)


# Ingesting the data into the folder named database as separate tables
def ingest_data(x,server_name,db_name):
    ServerName = str(server_name)
    DbName = str(db_name)
    engine = create_engine('mssql+pyodbc://' + ServerName + '/' + DbName + "?driver=SQL+Server")
    conn = engine.connect()
    for i in x.keys():
        TableName=i
        metadata = MetaData(conn)
        x[i].to_sql(TableName,engine,if_exists='replace',chunksize=500)
    conn.close()


ingest_data(x,'MSI\SQLEXPRESS', c)


# Importing the name of the tables and their descriptions and remarks given by the user into the master db
def master_db_inputs(b,server_name,db_name):
    ServerName = str(server_name)
    DbName = str(db_name)
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          f'Server={ServerName};'
                          f'Database={DbName};'
                          "Trusted_connection=yes;")
    conn.autocommit=True
    cursor=conn.cursor()
    for item in b:
        item1=item[:-4]
        description = input("Give a brief description of the table : ")
        remarks = input("Provide some remarks : ")
        #sql_query1 = "INSERT INTO Table_info (Name_Db, Description_Db, Remarks_Db) "
        #sql_query2 = "VALUES ("
        sql = "INSERT INTO Table_info VALUES('{}', '{}', '{}');".format(item1,description,remarks)
        cursor.execute(sql)
    conn.commit()
    conn.close() 


master_db_inputs(b,'MSI\SQLEXPRESS','MasterDB')

