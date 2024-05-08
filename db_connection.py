from dotenv import load_dotenv
import os
import pyodbc

load_dotenv(".env")

def databaseConnection():
   
    connectionString = f'DRIVER={os.getenv("ODBC_DRIVER")};SERVER={os.getenv("SERVER")};DATABASE={os.getenv("DATABASE")};UID={os.getenv("USERNAME")};PWD={os.getenv("PASSWORD")}'
    
    try:
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()
        return conn, cursor
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

def closeConnection(conn, cursor):
 
    if cursor:
        cursor.close()
    if conn:
        conn.close()
