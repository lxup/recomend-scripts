import os
import psycopg2
from datetime import datetime

def connect() -> psycopg2.extensions.connection:
    url = os.getenv("POSTGRES_CONNECTION_STRING")
    if not url:
        raise Exception("POSTGRES_CONNECTION_STRING is not set")
        
    return psycopg2.connect(url)    

def get_table(conn: psycopg2.extensions.connection, table_name: str, columns: list) -> list:
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
            return cursor.fetchall()
    except Exception as e:
        conn.rollback()
        raise e

def execute_query(conn: psycopg2.extensions.connection, query: str, params: tuple):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()  # Valider la transaction
    except Exception as e:
        conn.rollback()  # Annuler la transaction en cas d'erreur
        raise e

def insert_sync_log(conn: psycopg2.extensions.connection, date: datetime, sync_type: str, success: bool):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO tmdb_update_logs (date, success, type) VALUES ('{date}', {success}, '{sync_type}')")
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e