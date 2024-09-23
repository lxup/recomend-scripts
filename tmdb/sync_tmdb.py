# ---------------------------------------------------------------------------- #
#                                    Library                                   #
# ---------------------------------------------------------------------------- #

from rich.console import Console
from rich.theme import Theme
from rich.progress import track
from rich.progress import Progress
from rich.progress import TaskID
from tqdm import tqdm
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import requests
import gzip
import shutil
import json
from more_itertools import chunked
from concurrent.futures import ThreadPoolExecutor

import utils.utils as utils

consoleTheme = Theme({"success": "bold green", "error": "bold red", "warning": "bold yellow", "info": "white"})
console = Console(theme=consoleTheme)

load_dotenv()

# ---------------------------------------------------------------------------- #
#                               Global variables                               #
# ---------------------------------------------------------------------------- #

start_time = None
end_time = None
env_postgres_connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
env_tmdb_api_keys = os.getenv("TMDB_API_KEYS").split(",")
tmdb_api_key_index = 0
batch_size = 100
MAX_WORKERS = 10
csv_data = None

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                     Utils                                    #
# ---------------------------------------------------------------------------- #

def download_file(url: str) -> str:
	console.log(f"Downloading {url}", style="info")
	response = requests.get(url)

	if response.status_code != 200:
		console.log(f"Failed to download {url} (status code: {response.status_code})", style="error")
		return None
	
	file_name = url.split("/")[-1]

	with open(file_name, 'wb') as file:
		file.write(response.content)

	console.log(f"Downloaded {url} to {file_name}", style="success")
	return file_name

def decompress_gzip(file_path: str) -> str:
	console.log(f"Decompressing {file_path}", style="info")
	with gzip.open(file_path, 'rb') as f_in:
		with open(file_path[:-3], 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)

	console.log(f"Decompressed {file_path} to {file_path[:-3]}", style="success")
	return file_path[:-3]


# Get TMDB data
def get_tmdb_data(endpoint, params):
	global tmdb_api_key_index
	url = f"https://api.themoviedb.org/3/{endpoint}"
	params["api_key"] = env_tmdb_api_keys[tmdb_api_key_index]
	response = requests.get(url, params=params)
	tmdb_api_key_index = (tmdb_api_key_index + 1) % len(env_tmdb_api_keys)

	data = response.json()

	if ('success' in data and not data['success']):
		return None

	return data

def get_tmdb_export_ids(type: str, date: datetime) -> list:
	tmdb_export_collection_url_template = "http://files.tmdb.org/p/exports/{type}_ids_{date}.json.gz"
	
	tmdb_export_collection_ids_url = tmdb_export_collection_url_template.format(type=type,date=date.strftime("%m_%d_%Y"))

	downloaded_file = download_file(tmdb_export_collection_ids_url)
	if not downloaded_file:
		return None
	
	decompressed_file = decompress_gzip(downloaded_file)
	
	if os.path.exists(downloaded_file):
		os.remove(downloaded_file)

	with open(decompressed_file, 'r', encoding='utf-8') as file:
		data = [json.loads(line) for line in file]

	if os.path.exists(decompressed_file):
		os.remove(decompressed_file)

	if not data or len(data) == 0:
		return None

	return data

# ------------------------------------ DB ------------------------------------ #

def get_table(table_name: str, columns: list) -> list:
	conn = psycopg2.connect(env_postgres_connection_string)
	cursor = conn.cursor()
	rows = []
	try:
		cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
		rows = cursor.fetchall()
	finally:
		cursor.close()
		conn.close()
	return rows

def make_query(sql_command, values=None, fetch_results=False):
	conn = psycopg2.connect(env_postgres_connection_string)
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
		conn.close()

def get_last_sync(sync_type: str) -> datetime:
	conn = psycopg2.connect(env_postgres_connection_string)
	cursor = conn.cursor()
	try:
		cursor.execute(f"SELECT date FROM tmdb_update_logs WHERE type = '{sync_type}' AND success = True ORDER BY date DESC LIMIT 1")
		rows = cursor.fetchone()
		if rows:
			return rows[0]
		return None
	finally:
		cursor.close()
		conn.close()

def insert_sync_log(date: datetime, sync_type: str, success: bool):
	conn = psycopg2.connect(env_postgres_connection_string)
	cursor = conn.cursor()
	try:
		cursor.execute(f"INSERT INTO tmdb_update_logs (date, success, type) VALUES ('{date}', {success}, '{sync_type}')")
		conn.commit()
	finally:
		cursor.close()
		conn.close()

# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
#                                Sync functions                                #
# ---------------------------------------------------------------------------- #

# ---------------------------- Sync TMDB Language ---------------------------- #
def sync_tmdb_language() -> None:
	try:
		console.log("[sync_tmdb_language] Starting syncing tmdb_language", style="info")

		db_list: list = get_table("tmdb_language", ["iso_639_1"])
		if not db_list:
			raise Exception("Failed to download tmdb_language")

		tmdb_list: list = get_tmdb_data("configuration/languages", {})
		if not tmdb_list:
			raise Exception("Failed to get TMDB languages")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["iso_639_1"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_language] Found {len(missing_in_tmdb)} extra languages in db", style="warning")
			make_query("DELETE FROM tmdb_language WHERE iso_639_1 IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb

		# Insert missing in db
		with psycopg2.connect(env_postgres_connection_string) as conn:
			with conn.cursor() as cursor:
				try:
					console.log(f"[sync_tmdb_language] Found {len(missing_in_db)} missing languages in db", style="warning")
					# Démarrez la transaction
					conn.autocommit = False

					# Construire les valeurs à insérer dans Supabase pour tmdb_language
					values_to_insert_language = [
						(language['iso_639_1'], language['name'])
						for language in tmdb_list
						if language['iso_639_1'] in tmdb_set
					]
					# Construire les valeurs à insérer dans Supabase pour tmdb_language_translation
					values_to_insert_translation = [
						(language['iso_639_1'], 'en', language['english_name'])
						for language in tmdb_list
						if language['iso_639_1'] in tmdb_set
					]

					# Insérer les valeurs dans Supabase pour tmdb_language en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_language (iso_639_1, name_in_native_language)
						VALUES (%s, %s)
						ON CONFLICT (iso_639_1) DO UPDATE
						SET name_in_native_language = EXCLUDED.name_in_native_language
					""", values_to_insert_language)

					# Insérer les valeurs dans Supabase pour tmdb_language_translation en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_language_translation (iso_639_1, language, name)
						VALUES (%s, %s, %s)
						ON CONFLICT (iso_639_1, language) DO UPDATE
						SET name = EXCLUDED.name
					""", values_to_insert_translation)

					# Valider les modifications
					conn.commit()

					db_set.update(missing_in_db)

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					conn.rollback()
					console.log(f"Inserting tmdb_language: {e}", style="error")

				finally:
					# Rétablissez le mode autocommit à True
					conn.autocommit = True
		# Create CSV file for tmdb_language
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_language.csv", db_set)
		# Insert sync log
		insert_sync_log(start_time, "language", True)
	except Exception as e:
		insert_sync_log(start_time, "language", False)
		raise Exception(f"(sync_tmdb_language) {e}")

# ----------------------------- Sync TMDB Country ---------------------------- #
def sync_tmdb_country() -> None:
	try:
		console.log("[sync_tmdb_country] Starting syncing tmdb_country", style="info")

		db_list: list = get_table("tmdb_country", ["iso_3166_1"])
		if not db_list:
			raise Exception("Failed to download tmdb_country")

		tmdb_list: list = get_tmdb_data("configuration/countries", {"language": "fr-FR"})
		if not tmdb_list:
			raise Exception("Failed to get TMDB countries")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["iso_3166_1"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_country] Found {len(missing_in_tmdb)} extra countries in db", style="warning")
			make_query("DELETE FROM tmdb_country WHERE iso_3166_1 IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb
		
		with psycopg2.connect(env_postgres_connection_string) as conn:
			with conn.cursor() as cursor:
				try:
					# Démarrez la transaction
					conn.autocommit = False

					# Insert missing in db
					if missing_in_db:
						console.log(f"[sync_tmdb_country] Found {len(missing_in_db)} missing countries in db", style="warning")
						cursor.execute("""
							INSERT INTO tmdb_country (iso_3166_1)
							VALUES %s
							ON CONFLICT (iso_3166_1) DO NOTHING
						""", (tuple(missing_in_db),))

					# Build values to insert in DB
					values_to_insert_translation = [
						(country['iso_3166_1'], 'en', country['english_name'])
						for country in tmdb_list
						if country['iso_3166_1'] in tmdb_set
					] + [
						(country['iso_3166_1'], 'fr', country['native_name'])
						for country in tmdb_list
						if country['iso_3166_1'] in tmdb_set
					]

					# Insert values in DB
					cursor.executemany("""
						INSERT INTO tmdb_country_translation (iso_3166_1, iso_639_1, name)
						VALUES (%s, %s, %s)
						ON CONFLICT (iso_3166_1, iso_639_1) DO UPDATE
						SET name = EXCLUDED.name
					""", values_to_insert_translation)

					# Valider les modifications
					conn.commit()

					db_set.update(missing_in_db)

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					conn.rollback()
					console.log(f"Inserting tmdb_country: {e}", style="error")

				finally:
					# Rétablissez le mode autocommit à True
					conn.autocommit = True

		# Create CSV file for tmdb_country
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_country.csv", db_set)
		# Insert sync log
		insert_sync_log(start_time, "country", True)
	except Exception as e:
		insert_sync_log(start_time, "country", False)
		raise Exception(f"(sync_tmdb_country) {e}")

# ------------------------------ Sync TMDB Genre ----------------------------- #
def get_tmdb_genre(genre_type: str) -> dict:
	genres_en = get_tmdb_data(f"genre/{genre_type}/list", {"language": "en-US"})
	genres_fr = get_tmdb_data(f"genre/{genre_type}/list", {"language": "fr-FR"})
	if (genres_en is None or genres_fr is None):
		return None
	return {
		"en": genres_en["genres"],
		"fr": genres_fr["genres"]
	}

def sync_tmdb_genre() -> None:
	try:
		console.log("[sync_tmdb_genre] Starting syncing tmdb_genre", style="info")

		db_list: list = get_table("tmdb_genre", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_genre")

		tmdb_movie_dict: dict = get_tmdb_genre("movie")
		if not tmdb_movie_dict:
			raise Exception("Failed to get TMDB movie genres")
		tmdb_tv_dict: dict = get_tmdb_genre("tv")
		if not tmdb_tv_dict:
			raise Exception("Failed to get TMDB tv genres")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {genre['id'] for genre in tmdb_movie_dict['en'] + tmdb_tv_dict['en']}

		# Get difference between db and tmdb
		missing_in_db = tmdb_set - db_set
		missing_in_tmdb = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_genre] Found {len(missing_in_tmdb)} extra genres in db", style="warning")
			make_query("DELETE FROM tmdb_genre WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb
		
		with psycopg2.connect(env_postgres_connection_string) as conn:
			with conn.cursor() as cursor:
				try:
					# Démarrez la transaction
					conn.autocommit = False

					# Insert missing in db
					if missing_in_db:
						console.log(f"[sync_tmdb_genre] Found {len(missing_in_db)} missing genres in db", style="warning")
						cursor.execute("""
							INSERT INTO tmdb_genre (id)
							VALUES %s
							ON CONFLICT (id) DO NOTHING
						""", (tuple(missing_in_db),))

					values_to_insert_translation = [
						(genre['id'], 'en', genre['name']) for genre in tmdb_movie_dict["en"] + tmdb_tv_dict["en"]
					] + [
						(genre['id'], 'fr', genre['name']) for genre in tmdb_movie_dict["fr"] + tmdb_tv_dict["fr"]
					]
					
					# Insert values in DB
					cursor.executemany("""
						INSERT INTO tmdb_genre_translation (genre, language, name)
						VALUES (%s, %s, %s)
						ON CONFLICT (genre, language) DO UPDATE
						SET name = EXCLUDED.name
					""", values_to_insert_translation)

					# # Valider les modifications
					conn.commit()

					db_set.update(missing_in_db)
				except Exception as e:
					conn.rollback()
					console.log(f"Inserting tmdb_genre: {e}", style="error")
				finally:
					conn.autocommit = True

		# Create CSV file for tmdb_genre
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_genre.csv", db_set)
		# Insert sync log
		insert_sync_log(start_time, "genre", True)
	except Exception as e:
		insert_sync_log(start_time, "genre", False)
		raise Exception(f"(sync_tmdb_genre) {e}")

# ---------------------------------------------------------------------------- #

# ----------------------------- Sync TMDB Keyword ---------------------------- #
def sync_tmdb_keyword() -> None:
	try:
		console.log("[sync_tmdb_keyword] Starting syncing tmdb_keyword", style="info")

		db_list: list = get_table("tmdb_keyword", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_keyword")

		tmdb_list: list = get_tmdb_export_ids("keyword", start_time)
		if not tmdb_list:
			raise Exception("Failed to get TMDB keywords")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["id"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_keyword] Found {len(missing_in_tmdb)} extra keywords in db", style="warning")
			make_query("DELETE FROM tmdb_keyword WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb
		
		if missing_in_db:
			console.log(f"[sync_tmdb_keyword] Found {len(missing_in_db)} missing keywords in db", style="warning")
			with psycopg2.connect(env_postgres_connection_string) as conn:
				with conn.cursor() as cursor:
					try:
						# Démarrez la transaction
						conn.autocommit = False

						values_to_insert_keyword = [
							(keyword['id'], keyword['name'])
							for keyword in tmdb_list
							if keyword['id'] in missing_in_db
						]
						
						# Push in db
						cursor.executemany("""
							INSERT INTO tmdb_keyword (id, name)
							VALUES (%s, %s)
							ON CONFLICT (id) DO UPDATE
							SET name = EXCLUDED.name
						""", values_to_insert_keyword)

						# Valider les modifications
						conn.commit()

						db_set.update(missing_in_db)

					except Exception as e:
						# En cas d'erreur, annulez la transaction
						conn.rollback()
						console.log(f"Inserting tmdb_keyword: {e}", style="error")

					finally:
						# Rétablissez le mode autocommit à True
						conn.autocommit = True
		
		# Create CSV file for tmdb_keyword
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_keyword.csv", db_set)

		# Insert sync log
		insert_sync_log(start_time, "keyword", True)
	except Exception as e:
		insert_sync_log(start_time, "keyword", False)
		raise Exception(f"(sync_tmdb_keyword) {e}")
			
# ---------------------------------------------------------------------------- #

# --------------------------- Sync TMDB Collection --------------------------- #
def get_tmdb_collection(collection_id: int) -> dict:
	collection_en = get_tmdb_data(f"collection/{collection_id}", {"language": "en-US"})
	collection_fr = get_tmdb_data(f"collection/{collection_id}", {"language": "fr-FR"})
	if (collection_en is None or collection_fr is None):
		return None
	return {
		"en": collection_en,
		"fr": collection_fr
	}

def sync_tmdb_collection() -> None:
	try:
		console.log("[sync_tmdb_collection] Starting syncing tmdb_collection", style="info")

		db_list: list = get_table("tmdb_collection", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_collection")
		
		tmdb_list: list = get_tmdb_export_ids("collection", start_time)
		if not tmdb_list:
			raise Exception("Failed to get TMDB collections")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["id"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_collection] Found {len(missing_in_tmdb)} extra collections in db", style="warning")
			make_query("DELETE FROM tmdb_collection WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb

		if missing_in_db:
			console.log(f"[sync_tmdb_collection] Found {len(missing_in_db)} missing collections in db", style="warning")
			
			# Insert in chunks of 500
			chunks = list(chunked(missing_in_db, batch_size))

			# Add main progress bar for chunks and sub progress bar for collections
			with Progress() as progress:
				main_task: TaskID = progress.add_task("[cyan]Processing chunks", total=len(chunks))
				for chunk in chunks:
					with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
						items_to_insert = []
						futures = []

						# Re-use sub progress bar for each chunk
						collections_task: TaskID = progress.add_task(f"Processing collections", total=len(chunk))
						for collection in chunk:
							futures.append(executor.submit(get_tmdb_collection, collection))

						for future in futures:
							collection = future.result()
							if collection:
								items_to_insert.append(collection)
							progress.update(collections_task, advance=1)
						
						with psycopg2.connect(env_postgres_connection_string) as conn:
							with conn.cursor() as cursor:
								try:
									# Démarrez la transaction
									conn.autocommit = False

									values_to_insert_collection = [
										{
											'id': collection_data['en']['id'],
											'backdrop_path': collection_data['en'].get('backdrop_path', None),
										}
										for collection_data in items_to_insert
									]

									cursor.executemany("""
										INSERT INTO tmdb_collection (id, backdrop_path)
										VALUES (%(id)s, %(backdrop_path)s)
										ON CONFLICT (id) DO NOTHING
									""", values_to_insert_collection)

									values_to_insert_translations = [
										{
											'collection': collection_data['en']['id'],
											'language': 'en',
											'overview': collection_data['en'].get('overview', None),
											'poster_path': collection_data['en'].get('poster_path', None),
											'name': collection_data['en'].get('name', None),
										}
										for collection_data in items_to_insert
									] + [
										{
											'collection': collection_data['fr']['id'],
											'language': 'fr',
											'overview': collection_data['fr'].get('overview', None),
											'poster_path': collection_data['fr'].get('poster_path', None),
											'name': collection_data['fr'].get('name', None),
										}
										for collection_data in items_to_insert
									]

									cursor.executemany("""
										INSERT INTO tmdb_collection_translation (collection, language, overview, poster_path, name)
										VALUES (%(collection)s, %(language)s, %(overview)s, %(poster_path)s, %(name)s)
										ON CONFLICT (collection, language) DO UPDATE
										SET overview = EXCLUDED.overview,
											poster_path = EXCLUDED.poster_path,
											name = EXCLUDED.name
									""", values_to_insert_translations)

									conn.commit()

									# Add chunk to db_set
									db_set.update(set(chunk))
								except Exception as e:
									conn.rollback()
									console.log(f"Inserting tmdb_collection: {e}", style="error")
								finally:
									conn.autocommit = True
					progress.update(main_task, advance=1)
					progress.remove_task(collections_task)

				progress.remove_task(main_task)
		# Create CSV file for tmdb_collection
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_collection.csv", db_set)

		# Insert sync log
		insert_sync_log(start_time, "collection", True)
	except Exception as e:
		insert_sync_log(start_time, "collection", False)
		raise Exception(f"(sync_tmdb_collection) {e}")

# ---------------------------------------------------------------------------- #

# ----------------------------- Sync TMDB Company ---------------------------- #
def get_tmdb_company(company_id: int) -> dict:
	company = get_tmdb_data(f"company/{company_id}", {})
	if (company is None):
		return None
	return company

def sync_tmdb_company() -> None:
	try:
		console.log("[sync_tmdb_company] Starting syncing tmdb_company", style="info")

		db_list: list = get_table("tmdb_company", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_company")
		
		tmdb_list: list = get_tmdb_export_ids("production_company", start_time)
		if not tmdb_list:
			raise Exception("Failed to get TMDB companies")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["id"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_company] Found {len(missing_in_tmdb)} extra companies in db", style="warning")
			make_query("DELETE FROM tmdb_company WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb

		if missing_in_db:
			console.log(f"[sync_tmdb_company] Found {len(missing_in_db)} missing companies in db", style="warning")
			
			# Insert in chunks of 500
			chunks = list(chunked(missing_in_db, batch_size))

			# Add main progress bar for chunks and sub progress bar for companies
			with Progress() as progress:
				main_task: TaskID = progress.add_task("[cyan]Processing chunks", total=len(chunks))
				for chunk in chunks:
					with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
						items_to_insert = []
						futures = []

						# Re-use sub progress bar for each chunk
						companies_task: TaskID = progress.add_task(f"Processing companies", total=len(chunk))
						for company in chunk:
							futures.append(executor.submit(get_tmdb_company, company))

						for future in futures:
							company = future.result()
							if company:
								items_to_insert.append(company)
							progress.update(companies_task, advance=1)
						
						with psycopg2.connect(env_postgres_connection_string) as conn:
							with conn.cursor() as cursor:
								try:
									# Démarrez la transaction
									conn.autocommit = False

									values_to_insert_company = [
										{
											'id': company_data['id'],
											'name': company_data.get('name', None),
											'description': company_data.get('description', None),
											'headquarters': company_data.get('headquarters', None),
											'homepage': company_data.get('homepage', None),
											'logo_path': company_data.get('logo_path', None),
											'origin_country': company_data.get('origin_country', None),
											'parent_company': company_data.get('parent_company', None),
										}
										for company_data in items_to_insert
									]

									cursor.executemany("""
										INSERT INTO tmdb_company (id, name, description, headquarters, homepage, logo_path, origin_country, parent_company)
										VALUES (%(id)s, %(name)s, %(description)s, %(headquarters)s, %(homepage)s, %(logo_path)s, %(origin_country)s, %(parent_company)s)
										ON CONFLICT (id) DO UPDATE
										SET
											name = EXCLUDED.name,
											description = EXCLUDED.description,
											headquarters = EXCLUDED.headquarters,
											homepage = EXCLUDED.homepage,
											logo_path = EXCLUDED.logo_path,
											origin_country = EXCLUDED.origin_country,
											parent_company = EXCLUDED.parent_company
									""", values_to_insert_company)

									conn.commit()

									# Add chunk to db_set
									db_set.update(set(chunk))
								except Exception as e:
									conn.rollback()
									console.log(f"Inserting tmdb_company: {e}", style="error")
								finally:
									conn.autocommit = True
					progress.update(main_task, advance=1)
					progress.remove_task(companies_task)

				progress.remove_task(main_task)
		# Create CSV file for tmdb_company
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_company.csv", db_set)

		# Insert sync log
		insert_sync_log(start_time, "company", True)
	except Exception as e:
		insert_sync_log(start_time, "company", False)
		raise Exception(f"(sync_tmdb_company) {e}")

# ----------------------------- Sync TMDB Person ----------------------------- #
def get_tmdb_person(person_id: int) -> dict:
	person_en = get_tmdb_data(f"person/{person_id}", {"language": "en-US"})
	person_fr = get_tmdb_data(f"person/{person_id}", {"language": "fr-FR"})

	if (person_en is None or person_fr is None):
		return None
	return {
		"en": person_en,
		"fr": person_fr
	}

def update_db_person(persons_to_update: list) -> None:
	try:
		with psycopg2.connect(env_postgres_connection_string) as conn:
			with conn.cursor() as cursor:
				try:
					# Démarrez la transaction
					conn.autocommit = False

					values_to_insert_person = [
						{
							'id': person_data['en']['id'],
							'adult': person_data['en'].get('adult', False),
							'also_known_as': person_data['en'].get('also_known_as', []),
							'birthday': person_data['en'].get('birthday', None),
							'deathday': person_data['en'].get('deathday', None),
							'gender': person_data['en'].get('gender', None),
							'homepage': person_data['en'].get('homepage', None),
							'imdb_id': person_data['en'].get('imdb_id', None),
							'known_for_department': person_data['en'].get('known_for_department', None),
							'name': person_data['en'].get('name', None),
							'place_of_birth': person_data['en'].get('place_of_birth', None),
							'popularity': person_data['en'].get('popularity', None),
							'profile_path': person_data['en'].get('profile_path', None),
						}
						for person_data in persons_to_update
					]

					cursor.executemany("""
						INSERT INTO tmdb_person (id, adult, also_known_as, birthday, deathday, gender, homepage, imdb_id, known_for_department, name, place_of_birth, popularity, profile_path)
						VALUES (%(id)s, %(adult)s, %(also_known_as)s, %(birthday)s, %(deathday)s, %(gender)s, %(homepage)s, %(imdb_id)s, %(known_for_department)s, %(name)s, %(place_of_birth)s, %(popularity)s, %(profile_path)s)
						ON CONFLICT (id) DO UPDATE
						SET
							adult = EXCLUDED.adult,
							also_known_as = EXCLUDED.also_known_as,
							birthday = EXCLUDED.birthday,
							deathday = EXCLUDED.deathday,
							gender = EXCLUDED.gender,
							homepage = EXCLUDED.homepage,
							imdb_id = EXCLUDED.imdb_id,
							known_for_department = EXCLUDED.known_for_department,
							name = EXCLUDED.name,
							place_of_birth = EXCLUDED.place_of_birth,
							popularity = EXCLUDED.popularity,
							profile_path = EXCLUDED.profile_path
					""", values_to_insert_person)


					values_to_insert_person_translations = [
						{
							'person': person_data['en']['id'],
							'language': 'en',
							'biography': person_data['en'].get('biography', None),
						}
						for person_data in persons_to_update
					] + [
						{
							'person': person_data['fr']['id'],
							'language': 'fr',
							'biography': person_data['fr'].get('biography', None),
						}
						for person_data in persons_to_update
					]

					cursor.executemany("""
						INSERT INTO tmdb_person_translation (person, language, biography)
						VALUES (%(person)s, %(language)s, %(biography)s)
						ON CONFLICT (person, language) DO UPDATE
						SET biography = EXCLUDED.biography
					""", values_to_insert_person_translations)

					conn.commit()
				
				except Exception as e:
					conn.rollback()
					console.log(f"Inserting tmdb_person: {e}", style="error")
				finally:
					conn.autocommit = True
	except Exception as e:
		console.log(f"Inserting tmdb_person: {e}", style="error")
	
def sync_tmdb_person_daily_export() -> None:
	try:
		console.log("[sync_tmdb_person_daily_export] Starting syncing tmdb_person daily export", style="info")

		db_list: list = get_table("tmdb_person", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_person")
		
		tmdb_list: list = get_tmdb_export_ids("person", start_time)
		if not tmdb_list:
			raise Exception("Failed to get TMDB persons")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["id"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_person_daily_export] Found {len(missing_in_tmdb)} extra persons in db", style="warning")
			make_query("DELETE FROM tmdb_person WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb

		if missing_in_db:
			console.log(f"[sync_tmdb_person_daily_export] Found {len(missing_in_db)} missing persons in db", style="warning")
			
			# Insert in chunks of 500
			chunks = list(chunked(missing_in_db, batch_size))

			# Add main progress bar for chunks and sub progress bar for persons
			with Progress() as progress:
				main_task: TaskID = progress.add_task("[cyan]Processing chunks", total=len(chunks))
				for chunk in chunks:
					with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
						items_to_insert: list = []
						futures: list = []

						# Re-use sub progress bar for each chunk
						persons_task: TaskID = progress.add_task(f"Processing persons", total=len(chunk))
						for person in chunk:
							futures.append(executor.submit(get_tmdb_person, person))

						for future in futures:
							person = future.result()
							if person:
								items_to_insert.append(person)
							progress.update(persons_task, advance=1)
						
						update_db_person(items_to_insert)
						progress.update(main_task, advance=1)
						progress.remove_task(persons_task)

				progress.remove_task(main_task)
		# Create CSV file for tmdb_person
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_person.csv", db_set)
	except Exception as e:
		raise Exception(f"(sync_tmdb_person_daily_export) {e}")
	
def sync_tmdb_person_changes_export() -> None:
	try:
		console.log("[sync_tmdb_person_changes_export] Starting syncing tmdb_person changes export", style="info")

		last_sync_date = get_last_sync("person")
		if not last_sync_date:
			raise Exception("Failed to get last sync date")
		
		current_page = 1
		persons_to_update = []

		first_page = get_tmdb_data("person/changes", {"start_date": last_sync_date, "end_date": start_time.strftime("%Y-%m-%d"), "page": current_page})
		if not first_page:
			raise Exception("Failed to get TMDB person changes")
		total_to_update = first_page["total_results"]
		console.log(f"[sync_tmdb_person_changes_export] Found {total_to_update} persons to update between {last_sync_date} and {start_time.strftime('%Y-%m-%d')}", style="info")

		with Progress() as progress:
			main_task: TaskID = progress.add_task("[cyan]Processing persons", total=total_to_update)
			while True:
				changed_persons_response = get_tmdb_data("person/changes", {"start_date": last_sync_date, "end_date": start_time.strftime("%Y-%m-%d"), "page": current_page})
				if not changed_persons_response:
					raise Exception("Failed to get TMDB person changes")
				
				changed_persons = changed_persons_response["results"]
				if not changed_persons or len(changed_persons) == 0:
					break

				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = []
					for person in changed_persons:
						futures.append(executor.submit(get_tmdb_person, person['id']))

					for future in futures:
						person = future.result()
						if person:
							persons_to_update.append(person)
						progress.update(main_task, advance=1)
				
				current_page += 1

				# Update db every 5 pages
				if current_page % 5 == 0:
					if len(persons_to_update):
						update_db_person(persons_to_update)
					persons_to_update = []
			
			# Update db for remaining persons
			if len(persons_to_update):
				update_db_person(persons_to_update)

			progress.remove_task(main_task)

	except Exception as e:
		raise Exception(f"(sync_tmdb_person_changes_export) {e}")

def sync_tmdb_person() -> None:
	try:
		console.log("[sync_tmdb_person] Starting syncing tmdb_person", style="info")

		sync_tmdb_person_daily_export()
		sync_tmdb_person_changes_export()

		# Insert sync log
		insert_sync_log(start_time, "person", True)
	except Exception as e:
		insert_sync_log(start_time, "person", False)
		raise Exception(f"(sync_tmdb_person) {e}")

# ---------------------------------------------------------------------------- #


# ------------------------------ Sync TMDB Movie ----------------------------- #

def get_csv_data() -> None:
	global csv_data
	csv_data = {}
	try:
		csv_data['language'] = get_table("tmdb_language", ["iso_639_1"])
		csv_data['country'] = get_table("tmdb_country", ["iso_3166_1"])
		csv_data['genre'] = get_table("tmdb_genre", ["id"])
		csv_data['keyword'] = get_table("tmdb_keyword", ["id"])
		csv_data['collection'] = get_table("tmdb_collection", ["id"])
		csv_data['company'] = get_table("tmdb_company", ["id"])
		csv_data['person'] = get_table("tmdb_person", ["id"])
	except Exception as e:
		raise Exception(f"(get_csv_data) {e}")

def get_tmdb_movie(movie_id: int) -> dict:
	movie_en = get_tmdb_data(f"movie/{movie_id}", {"language": "en-US", "append_to_response": "credits,keywords,videos,belongs_to_collection"})
	movie_fr = get_tmdb_data(f"movie/{movie_id}", {"language": "fr-FR", "append_to_response": "credits,keywords,videos,belongs_to_collection"})
	if (movie_en is None or movie_fr is None):
		return None
	return {
		"english": movie_en,
		"french": movie_fr
	}

def update_db_movie(movies_to_update: list) -> None:
	try:
		if not csv_data:
			raise Exception("CSV data is empty")
		with psycopg2.connect(env_postgres_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					# Démarrez la transaction
					connection.autocommit = False

					# ========== START TMDB_MOVIE ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie
					values_to_insert_movie = [
						{
							'id': movie_data['english']['id'],
							'adult': movie_data['english'].get('adult', False),
							'backdrop_path': movie_data['english'].get('backdrop_path', None),
							'budget': movie_data['english'].get('budget', None),
							'homepage': movie_data['english'].get('homepage', None),
							'imdb_id': movie_data['english'].get('imdb_id', None),
							'original_language': movie_data['english'].get('original_language', None),
							'original_title': movie_data['english'].get('original_title', None),
							'popularity': movie_data['english'].get('popularity', None),
							'release_date': None if movie_data['english'].get('release_date') == '' else movie_data['english'].get('release_date', None),
							'revenue': movie_data['english'].get('revenue', None),
							'runtime': movie_data['english'].get('runtime', None),
							'status': movie_data['english'].get('status', None),
							'vote_average': movie_data['english'].get('vote_average', None),
							'vote_count': movie_data['english'].get('vote_count', None),
							'collection_id': movie_data['english']['belongs_to_collection']['id'] if movie_data['english'].get('belongs_to_collection') and movie_data['english']['belongs_to_collection']['id'] in csv_data['collection'] else None,						}
						for movie_data in movies_to_update
					]

					# Insérer les valeurs dans Supabase pour tmdb_movie en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie (id, adult, backdrop_path, budget, homepage, imdb_id, original_language, original_title, popularity, release_date, revenue, runtime, status, vote_average, vote_count, collection_id)
						VALUES (%(id)s, %(adult)s, %(backdrop_path)s, %(budget)s, %(homepage)s, %(imdb_id)s, %(original_language)s, %(original_title)s, %(popularity)s, %(release_date)s, %(revenue)s, %(runtime)s, %(status)s, %(vote_average)s, %(vote_count)s, %(collection_id)s)
						ON CONFLICT (id) DO UPDATE
						SET
							adult = EXCLUDED.adult,
							backdrop_path = EXCLUDED.backdrop_path,
							budget = EXCLUDED.budget,
							homepage = EXCLUDED.homepage,
							imdb_id = EXCLUDED.imdb_id,
							original_language = EXCLUDED.original_language,
							original_title = EXCLUDED.original_title,
							popularity = EXCLUDED.popularity,
							release_date = EXCLUDED.release_date,
							revenue = EXCLUDED.revenue,
							runtime = EXCLUDED.runtime,
							status = EXCLUDED.status,
							vote_average = EXCLUDED.vote_average,
							vote_count = EXCLUDED.vote_count,
							collection_id = EXCLUDED.collection_id
					""", values_to_insert_movie)
					
					# ========== END TMDB_MOVIE ========== #

					# ========== START TMDB_MOVIE_TRANSLATION ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_translation
					values_to_insert_movie_translations = [
						{
							'movie_id': movie_data['english']['id'],
							'language_id': 'en',
							'overview': movie_data['english'].get('overview', None),
							'poster_path': movie_data['english'].get('poster_path', None),
							'tagline': movie_data['english'].get('tagline', None),
							'title': movie_data['english'].get('title', None),
						}
						for movie_data in movies_to_update
					] + [
						{
							'movie_id': movie_data['french']['id'],
							'language_id': 'fr',
							'overview': movie_data['french'].get('overview', None),
							'poster_path': movie_data['french'].get('poster_path', None),
							'tagline': movie_data['french'].get('tagline', None),
							'title': movie_data['french'].get('title', None),
						}
						for movie_data in movies_to_update
					]

					# Insérer les valeurs dans Supabase pour tmdb_movie_translation en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_translation (movie_id, language_id, overview, poster_path, tagline, title)
						VALUES (%(movie_id)s, %(language_id)s, %(overview)s, %(poster_path)s, %(tagline)s, %(title)s)
						ON CONFLICT (movie_id, language_id) DO UPDATE
						SET
							overview = EXCLUDED.overview,
							poster_path = EXCLUDED.poster_path,
							tagline = EXCLUDED.tagline,
							title = EXCLUDED.title
					""", values_to_insert_movie_translations)

					# ========== END TMDB_MOVIE_TRANSLATION ========== #
			
					# ========== START TMDB_MOVIE_COUNTRY ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_country
					values_to_insert_movie_countries = [
						{
							'movie_id': movie_data['english']['id'],
							'country_id': country_data['iso_3166_1'],
						}
						for movie_data in movies_to_update
						for country_data in movie_data.get('english', {}).get('production_countries', [])
						if country_data['iso_3166_1'] in csv_data['country']
					]

					# Insérer les valeurs dans Supabase pour tmdb_movie_country en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_country (movie_id, country_id)
						VALUES (%(movie_id)s, %(country_id)s)
						ON CONFLICT (movie_id, country_id) DO NOTHING
					""", values_to_insert_movie_countries)
					
					# ========== END TMDB_MOVIE_COUNTRY ========== #

					# ========== START TMDB_MOVIE_CREDIT ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_credits
					values_to_insert_movie_credits = []
					for movie_data in movies_to_update:
						movie_id = movie_data['english']['id']
						credits_data = movie_data.get('english', {}).get('credits', {})
						
						if credits_data:
							cast = credits_data.get('cast', [])
							crew = credits_data.get('crew', [])

							# Traitement pour le cast
							values_cast = [
								{
									'id': actor.get('credit_id', None) if isinstance(actor, dict) else None,
									'movie_id': movie_id,
									'person_id': actor.get('id', None) if isinstance(actor, dict) else None,
									'department': 'Acting',
									'job': 'Actor'
								}
								for actor in cast
								if isinstance(actor, dict) and actor.get('id') in csv_data['person']
							]

							# Traitement pour le crew
							values_crew = [
								{
									'id': crew_member.get('credit_id', None) if isinstance(crew_member, dict) else None,
									'movie_id': movie_id,
									'person_id': crew_member.get('id', None) if isinstance(crew_member, dict) else None,
									'department': crew_member.get('department', None) if isinstance(crew_member, dict) else None,
									'job': crew_member.get('job', None) if isinstance(crew_member, dict) else None
								}
								for crew_member in crew
								if isinstance(crew_member, dict) and crew_member.get('id') in csv_data['person']
							]

							# Concaténer les valeurs pour avoir une seule liste de crédits
							values_to_insert_movie_credits.extend(values_cast)
							values_to_insert_movie_credits.extend(values_crew)

					# Insérer les valeurs dans Supabase pour tmdb_movie_credits en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_credits (id, movie_id, person_id, department, job)
						VALUES (%(id)s, %(movie_id)s, %(person_id)s, %(department)s, %(job)s)
						ON CONFLICT (id) DO NOTHING
					""", values_to_insert_movie_credits)
					
					# ========== END TMDB_MOVIE_CREDIT ========== #

					# ========== START TMDB_MOVIE_ROLE ========== #
					values_to_insert_movie_roles = []
					for movie_data in movies_to_update:
						credits_data = movie_data.get('english', {}).get('credits', {})
						
						if credits_data:
							cast = credits_data.get('cast', [])

							# Traitement pour les rôles
							values_roles = []
							for actor in cast:
								if isinstance(actor, dict):
									credit_id = actor.get('credit_id', None)
									if credit_id in [credit['id'] for credit in values_to_insert_movie_credits]:
										values_roles.append({
											'credit_id': credit_id,
											'character': actor.get('character', None),
											'order': actor.get('order', None),
										})

							# Concaténer les valeurs pour avoir une seule liste de rôles
							values_to_insert_movie_roles.extend(values_roles)
							
					# Insérer les valeurs dans Supabase pour tmdb_movie_role en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_role (credit_id, character, "order")
						VALUES (%(credit_id)s, %(character)s, %(order)s)
						ON CONFLICT (credit_id) DO NOTHING
					""", values_to_insert_movie_roles)
					
					# ========== END TMDB_MOVIE_ROLE ========== #

					# ========== START TMDB_MOVIE_GENRE ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_genre
					values_to_insert_movie_genres = []

					for movie_data in movies_to_update:
						genres_data = movie_data.get('english', {}).get('genres', [])

						# Traitement pour les genres
						values_genres = [
							{
								'movie_id': movie_data['english']['id'],
								'genre_id': genre.get('id', None) if isinstance(genre, dict) else None,
							}
							for genre in genres_data
							if isinstance(genre, dict) and genre.get('id') in csv_data['genre']
						]

						# Concaténer les valeurs pour avoir une seule liste de genres
						values_to_insert_movie_genres.extend(values_genres)
						
					# Insérer les valeurs dans Supabase pour tmdb_movie_genre en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_genre (movie_id, genre_id)
						VALUES (%(movie_id)s, %(genre_id)s)
						ON CONFLICT (movie_id, genre_id) DO NOTHING
					""", values_to_insert_movie_genres)
					
					# ========== END TMDB_MOVIE_GENRE ========== #

					# ========== START TMDB_MOVIE_KEYWORD ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_keyword
					values_to_insert_movie_keywords = []

					for movie_data in movies_to_update:
						keywords_data = movie_data.get('english', {}).get('keywords', [])

						# Traitement pour les mots-clés
						values_keywords = [
							{
								'movie_id': movie_data['english']['id'],
								'keyword_id': keyword.get('id', None) if isinstance(keyword, dict) else keyword.get('id', None),
							}
							for keyword in keywords_data.get('keywords', [])
							if isinstance(keyword, dict) and keyword.get('id') in csv_data['keyword']
						]

						# Concaténer les valeurs pour avoir une seule liste de mots-clés
						values_to_insert_movie_keywords.extend(values_keywords)
						
					# Insérer les valeurs dans Supabase pour tmdb_movie_keyword en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_keyword (movie_id, keyword_id)
						VALUES (%(movie_id)s, %(keyword_id)s)
						ON CONFLICT (movie_id, keyword_id) DO NOTHING
					""", values_to_insert_movie_keywords)
					
					# ========== END TMDB_MOVIE_KEYWORD ========== #

					# ========== START TMDB_MOVIE_LANGUAGE ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_language
					values_to_insert_movie_languages = []

					for movie_data in movies_to_update:
						languages_data = movie_data.get('english', {}).get('spoken_languages', [])

						# Traitement pour les langues
						values_languages = [
							{
								'movie_id': movie_data['english']['id'],
								'language_id': language.get('iso_639_1', None) if isinstance(language, dict) else None,
							}
							for language in languages_data
							if isinstance(language, dict) and language.get('iso_639_1') in csv_data['language']
						]

						# Concaténer les valeurs pour avoir une seule liste de langues
						values_to_insert_movie_languages.extend(values_languages)
						
					# Insérer les valeurs dans Supabase pour tmdb_movie_language en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_language (movie_id, language_id)
						VALUES (%(movie_id)s, %(language_id)s)
						ON CONFLICT (movie_id, language_id) DO NOTHING
					""", values_to_insert_movie_languages)
					
					# ========== END TMDB_MOVIE_LANGUAGE ========== #

					# ========== START TMDB_MOVIE_PRODUCTION ========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_production
					values_to_insert_movie_production = []

					for movie_data in movies_to_update:
						production_companies_data = movie_data.get('english', {}).get('production_companies', [])

						# Traitement pour les sociétés de production
						values_production = [
							{
								'movie_id': movie_data['english']['id'],
								'company_id': company.get('id', None) if isinstance(company, dict) else None,
							}
							for company in production_companies_data
							if isinstance(company, dict) and company.get('id') in csv_data['company']
						]

						# Concaténer les valeurs pour avoir une seule liste de sociétés de production
						values_to_insert_movie_production.extend(values_production)

					# Insérer les valeurs dans Supabase pour tmdb_movie_production en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_production (movie_id, company_id)
						VALUES (%(movie_id)s, %(company_id)s)
						ON CONFLICT (movie_id, company_id) DO NOTHING
					""", values_to_insert_movie_production)

					# ========== END TMDB_MOVIE_PRODUCTION ========== #

					# ========== START TMDB_MOVIE_VIDEOS========== #
					# Construire les valeurs à insérer dans Supabase pour tmdb_movie_videos
					values_to_insert_movie_videos = []

					for movie_data in movies_to_update:
						videos_data_en = movie_data.get('english', {}).get('videos', {}).get('results', [])
						videos_data_fr = movie_data.get('french', {}).get('videos', {}).get('results', [])

						# Filtrer les vidéos pour ne prendre que celles de type "Teaser" ou "Trailer"
						videos_en = [video for video in videos_data_en if video.get('iso_639_1') == 'en' and (video.get('type') == 'Teaser' or video.get('type') == 'Trailer')]
						videos_fr = [video for video in videos_data_fr if video.get('iso_639_1') == 'fr' and (video.get('type') == 'Teaser' or video.get('type') == 'Trailer')]

						# Traitement pour les vidéos en anglais
						values_videos_en = [
							{
								'id': video.get('id', None) if isinstance(video, dict) else None,
								'movie_id': movie_data['english']['id'],
								'iso_639_1': video.get('iso_639_1', None) if isinstance(video, dict) else None,
								'iso_3166_1': video.get('iso_3166_1', None) if isinstance(video, dict) else None,
								'name': video.get('name', None) if isinstance(video, dict) else None,
								'key': video.get('key', None) if isinstance(video, dict) else None,
								'site': video.get('site', None) if isinstance(video, dict) else None,
								'size': video.get('size', None) if isinstance(video, dict) else None,
								'type': video.get('type', None) if isinstance(video, dict) else None,
								'official': video.get('official', False) if isinstance(video, dict) else False,
							}
							for video in videos_en
						]

						# Traitement pour les vidéos en français
						values_videos_fr = [
							{
								'id': video.get('id', None) if isinstance(video, dict) else None,
								'movie_id': movie_data['french']['id'],
								'iso_639_1': video.get('iso_639_1', None) if isinstance(video, dict) else None,
								'iso_3166_1': video.get('iso_3166_1', None) if isinstance(video, dict) else None,
								'name': video.get('name', None) if isinstance(video, dict) else None,
								'key': video.get('key', None) if isinstance(video, dict) else None,
								'site': video.get('site', None) if isinstance(video, dict) else None,
								'size': video.get('size', None) if isinstance(video, dict) else None,
								'type': video.get('type', None) if isinstance(video, dict) else None,
								'official': video.get('official', False) if isinstance(video, dict) else False,
							}
							for video in videos_fr
						]

						# Concaténer les valeurs pour avoir une seule liste de vidéos
						values_to_insert_movie_videos.extend(values_videos_en)
						values_to_insert_movie_videos.extend(values_videos_fr)
						
					# Insérer les valeurs dans Supabase pour tmdb_movie_videos en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_movie_videos (id, movie_id, iso_639_1, iso_3166_1, name, key, site, size, type, official)
						VALUES (%(id)s, %(movie_id)s, %(iso_639_1)s, %(iso_3166_1)s, %(name)s, %(key)s, %(site)s, %(size)s, %(type)s, %(official)s)
						ON CONFLICT (id) DO NOTHING
					""", values_to_insert_movie_videos)		
					
					# ========== END TMDB_MOVIE_VIDEOS========== #

					# Valider les modifications
					connection.commit()

					print(f"Successfully updated {len(movies_to_update)} movies in Supabase")

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					connection.rollback()
					print(f"Error uploading TMDB movies in Supabase: {e}")

				finally:
					# Rétablissez le mode autocommit à True
					connection.autocommit = True
	except Exception as e:
		print(f"Error updating TMDB movies in Supabase: {e}")

def sync_tmdb_movie_daily_export() -> None:
	try:
		console.log("[sync_tmdb_movie_daily_export] Starting syncing tmdb_movie daily export", style="info")

		db_list: list = get_table("tmdb_movie", ["id"])
		if not db_list:
			raise Exception("Failed to download tmdb_movie")
		
		tmdb_list: list = get_tmdb_export_ids("movie", start_time)
		if not tmdb_list:
			raise Exception("Failed to get TMDB movies")
		
		db_set: set = {item[0] for item in db_list}
		tmdb_set: set = {item["id"] for item in tmdb_list}

		# Get difference between db and tmdb
		missing_in_db: set = tmdb_set - db_set
		missing_in_tmdb: set = db_set - tmdb_set

		# Delete missing in tmdb
		if missing_in_tmdb:
			console.log(f"[sync_tmdb_movie_daily_export] Found {len(missing_in_tmdb)} extra movies in db", style="warning")
			make_query("DELETE FROM tmdb_movie WHERE id IN %s", (tuple(missing_in_tmdb),))
			db_set -= missing_in_tmdb

		if missing_in_db:
			console.log(f"[sync_tmdb_movie_daily_export] Found {len(missing_in_db)} missing movies in db", style="warning")
			
			# Insert in chunks of 500
			chunks = list(chunked(missing_in_db, batch_size))

			# Add main progress bar for chunks and sub progress bar for movies
			with Progress() as progress:
				main_task: TaskID = progress.add_task("[cyan]Processing chunks", total=len(chunks))
				for chunk in chunks:
					with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
						items_to_insert: list = []
						futures: list = []

						# Re-use sub progress bar for each chunk
						movies_task: TaskID = progress.add_task(f"Processing movies", total=len(chunk))
						for movie in chunk:
							futures.append(executor.submit(get_tmdb_movie, movie))

						for future in futures:
							movie = future.result()
							if movie:
								items_to_insert.append(movie)
							progress.update(movies_task, advance=1)
						
						update_db_movie(items_to_insert)
						progress.update(main_task, advance=1)
						progress.remove_task(movies_task)

				progress.remove_task(main_task)
		# Create CSV file for tmdb_movie
		# utils.create_csv_file(os.getenv("TMP_DIR") + "/tmdb_movie.csv", db_set)		
	except Exception as e:
		raise Exception(f"(sync_tmdb_movie_daily_export) {e}")
	
def sync_tmdb_movie_changes_export() -> None:
	try:
		console.log("[sync_tmdb_movie_changes_export] Starting syncing tmdb_movie changes export", style="info")

		last_sync_date = get_last_sync("movie")
		if not last_sync_date:
			raise Exception("Failed to get last sync date")
		
		current_page = 1
		movies_to_update = []

		first_page = get_tmdb_data("movie/changes", {"start_date": last_sync_date, "end_date": start_time.strftime("%Y-%m-%d"), "page": current_page})
		if not first_page:
			raise Exception("Failed to get TMDB movie changes")
		total_to_update = first_page["total_results"]
		console.log(f"[sync_tmdb_movie_changes_export] Found {total_to_update} movies to update between {last_sync_date} and {start_time.strftime('%Y-%m-%d')}", style="info")

		with Progress() as progress:
			main_task: TaskID = progress.add_task("[cyan]Processing movies", total=total_to_update)
			while True:
				changed_movies_response = get_tmdb_data("movie/changes", {"start_date": last_sync_date, "end_date": start_time.strftime("%Y-%m-%d"), "page": current_page})
				if not changed_movies_response:
					raise Exception("Failed to get TMDB movie changes")
				
				changed_movies = changed_movies_response["results"]
				if not changed_movies or len(changed_movies) == 0:
					break

				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = []
					for movie in changed_movies:
						futures.append(executor.submit(get_tmdb_movie, movie['id']))

					for future in futures:
						movie = future.result()
						if movie:
							movies_to_update.append(movie)
						progress.update(main_task, advance=1)
				
				current_page += 1

				# Update db every 5 pages
				if current_page % 5 == 0:
					if len(movies_to_update):
						update_db_movie(movies_to_update)
					movies_to_update = []
			
			# Update db for remaining movies
			if len(movies_to_update):
				update_db_movie(movies_to_update)

			progress.remove_task(main_task)
	except Exception as e:
		raise Exception(f"(sync_tmdb_movie_changes_export) {e}")

def sync_tmdb_movie() -> None:
	try:
		console.log("[sync_tmdb_movie] Starting syncing tmdb_movie", style="info")

		# Récupérer les données CSV
		get_csv_data()

		sync_tmdb_movie_daily_export()
		sync_tmdb_movie_changes_export()

		# Insert sync log
		insert_sync_log(start_time, "movie", True)
	except Exception as e:
		insert_sync_log(start_time, "movie", False)
		raise Exception(f"(sync_tmdb_movie) {e}")

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                     Main                                     #
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
	# start_time = datetime.now()
	# set manual start time to 20 september 2024
	start_time = datetime(2024, 9, 20, 0, 0, 0)
	console.log("Starting sync TMDB with script v" + os.getenv("VERSION") + " at " + start_time.strftime("%Y-%m-%d %H:%M:%S"), style="info")
	try:
		# Check if TMDB API keys are set
		if (len(env_tmdb_api_keys) == 0):
			raise Exception("TMDB API keys are not set")
		# Create tmp directory
		utils.create_directory(os.getenv("TMP_DIR"))
		# Sync functions
		# sync_tmdb_language()
		# sync_tmdb_country()
		# sync_tmdb_genre()
		# sync_tmdb_keyword()
		# sync_tmdb_collection()
		# sync_tmdb_company()
		# sync_tmdb_person()
		sync_tmdb_movie()
	except Exception as e:
		console.log(f"Error: {e}", style="error")
		exit(1)
	finally:
		console.log("Sync TMDB finished in " + str(datetime.now() - start_time), style="info")
		if os.path.exists(os.getenv("TMP_DIR")):
			shutil.rmtree(os.getenv("TMP_DIR"))
