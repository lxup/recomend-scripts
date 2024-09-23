import psycopg2
from datetime import datetime

def connector(postgres_connection_string: str):
    if (len(postgres_connection_string) == 0):
        raise Exception("Postgres connection string is not set")
    conn = psycopg2.connect(postgres_connection_string)
    return conn

# ---------------------------------------------------------------------------- #

# Get entire postgres table
def get_table(conn, table_name: str, columns: list) -> list:
    cursor = conn.cursor()
    cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    return rows

# ---------------------------------------------------------------------------- #

# def make_query(conn, query: str):
#     cursor = conn.cursor()
#     cursor.execute(query)
#     output = cursor.fetchall()
#     cursor.close()
#     return output

def make_query(conn, sql_command, values=None, fetch_results=False):
	cursor = conn.cursor()
	try:
		if values:
			cursor.execute(sql_command, values)
		else:
			cursor.execute(sql_command)
		
		# Valider les modifications pour les commandes d'insertion, de suppression, etc.
		conn.commit()

		if fetch_results:
			result = cursor.fetchall()
			return result
	finally:
		cursor.close()

# ---------------------------------------------------------------------------- #

class SyncLog:
    def __init__(self, id: int, created_at: datetime, updated_at: datetime, sync_type: str, status: str):
        self.id = id
        self.created_at = created_at
        self.updated_at = updated_at
        self.sync_type = sync_type
        self.status = status

def get_last_sync(conn, sync_type: str) -> SyncLog:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM sync_logs WHERE type = '{sync_type}' ORDER BY created_at DESC LIMIT 1")
    rows = cursor.fetchone()
    cursor.close()
    return SyncLog(*rows)

def insert_sync_log(conn, sync_type: str, status: str):
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO sync_logs (type, status) VALUES ('{sync_type}', '{status}')")
    conn.commit()
    cursor.close()



