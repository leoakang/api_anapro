from dotenv import load_dotenv
import os
import requests
import io
import pandas as pd
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
from db_connection import databaseConnection, closeConnection

def main():
    load_dotenv(".env")

    baseUrl = os.getenv("BASE_URL")
    endpoint = os.getenv("ENDPOINT")
    requestUrl = baseUrl + endpoint

    timeStart = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        response = requests.get (
            requestUrl,
            params = {
                "token": os.getenv("TOKEN"),
                "nome":  os.getenv("NOME"),
                "subscription-key": os.getenv("SUBSCRIPTION-KEY")
            },
            headers= {
                "Authorization": os.getenv("AUTHORIZATION")
            }
        )

        response.raise_for_status()
        content_type = response.headers.get("Content-Type")

        if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in content_type:
            df = pd.read_excel(io.BytesIO(response.content))
            df = df.fillna('')
            df.insert(0, "DATAREF", timeStart)
            conn, _ = databaseConnection()
            engine = create_engine("mssql+pyodbc://", creator=lambda: conn)

            try:
                df.to_sql("AnaproVibra", engine, if_exists="append", index=False)
                print("Dados inseridos com sucesso na tabela AnaproVibra")
            except Exception as e:
                print(f"Erro ao inserir dados na tabela AnaproVibra: {e}")
            finally:
                closeConnection(conn, None)

        else:
            print("O conteúdo da resposta não é um arquivo .xlsx")

    except requests.exceptions.RequestException as e:
        print("Erro na solicitação:", e)

    except Exception as e:
        print("Erro:", e)

if __name__ == "__main__":
    main()
