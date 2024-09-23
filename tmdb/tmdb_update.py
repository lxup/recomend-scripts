
# ---------------------------------------------------------------------------- #
#                                    Library                                   #
# ---------------------------------------------------------------------------- #

from datetime import datetime, timedelta
import requests
import time
import gzip
import shutil
import json
import os
from dotenv import load_dotenv
from more_itertools import chunked
import psycopg2
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

load_dotenv()

# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                               Global variables                               #
# ---------------------------------------------------------------------------- #

supabase_connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
supabase_tmdb_changes_logs_table = "tmdb_update_logs"
tmdb_api_keys = os.getenv("TMDB_API_KEYS").split(",")
api_key_index = 0
MAX_WORKERS = 10

# ---------------------------------------------------------------------------- #

# ========== START TOOLS ========== #
def download_file(url: str) -> str:
	print(f"Downloading {url}")
	response = requests.get(url)

	if response.status_code != 200:
		print(f"Failed to download {url}. Status code: {response.status_code}")
		return None
	
	file_name = url.split("/")[-1]

	with open(file_name, 'wb') as file:
		file.write(response.content)

	print(f"Downloaded {file_name}")
	return file_name

def decompress_gzip(file_path: str) -> str:
	print(f"Decompressing {file_path}")
	with gzip.open(file_path, 'rb') as f_in:
		with open(file_path[:-3], 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)

	print(f"Decompressed {file_path[:-3]}")
	return file_path[:-3]

def supabase_tmdb_update_log(date: datetime, success: bool, type: str):
	try:
		with psycopg2.connect(supabase_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					log_values = {
						'date': date.date(),
						'success': success,
						'type': type,
					}
					cursor.execute("""
						INSERT INTO tmdb_update_logs (date, success, type)
						VALUES (%(date)s, %(success)s, %(type)s)
					""", log_values)

					connection.commit()

				except Exception as e:
					connection.rollback()
					print(f"Error adding log to tmdb_update_logs: {e}")
	except Exception as e:
		print(f"Error adding log to tmdb_update_logs: {e}")

def execute_sql_command(sql_command, values=None, fetch_results=False):
	connection = psycopg2.connect(supabase_connection_string)
	cursor = connection.cursor()
	try:
		if values:
			cursor.execute(sql_command, values)
		else:
			cursor.execute(sql_command)
		
		# Valider les modifications pour les commandes d'insertion, de suppression, etc.
		connection.commit()

		if fetch_results:
			result = cursor.fetchall()
			return result
	finally:
		cursor.close()
		connection.close()

def create_csv_file(file_name, data, append=False):
	mode = 'a' if append else 'w'

	with open(file_name, mode, newline='', encoding='utf-8') as file:
		writer = csv.writer(file)

		# Si on crée un nouveau fichier, écrire l'en-tête
		if not append:
			# Utiliser la liste d'ISO 639-1 comme en-tête
			header = ['id']
			writer.writerow(header)

		# Écrire toutes les valeurs de data
		for value in data:
			writer.writerow([value])

def load_csv_file(file_name):
	data_set = set()
	try:
		with open(file_name, 'r', newline='', encoding='utf-8') as file:
			reader = csv.reader(file)
			next(reader)  # Skip header
			for row in reader:
				if row:  # Ensure row is not empty
					data_set.add(row[0])
	except FileNotFoundError:
		pass  # Handle the case where the file doesn't exist
	return data_set

# ========== END TOOLS ========== #

# ========== START TMDB ========== #
def get_tmdb_data(url: str, params) -> dict:
	global api_key_index
	params["api_key"] = tmdb_api_keys[api_key_index]
	response = requests.get(url, params=params)
	response.raise_for_status()
	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	data = response.json()

	if ('success' in data and not data['success']):
		print(f"L'appel à l'API TMDB a échoué. Code d'erreur: {data['status_code']}")
		return None
	
	return data

def get_tmdb_export_ids(type: str, date: datetime):
	tmdb_export_collection_url_template = "http://files.tmdb.org/p/exports/{type}_ids_{date}.json.gz"
	
	tmdb_export_collection_ids_url = tmdb_export_collection_url_template.format(type=type,date=date.strftime("%m_%d_%Y"))

	downloaded_file = download_file(tmdb_export_collection_ids_url)
	if not downloaded_file:
		return None
	
	decompressed_file = decompress_gzip(downloaded_file)
	
	if os.path.exists(downloaded_file):
		os.remove(downloaded_file)

	return decompressed_file
# ========== END TMDB ========== #

# ========== START TMDB CONFIGURATION ========== #

def tmdb_update_language(current_date: datetime, file_name: str = "tmdb_language.csv"):
	try:
		print("Starting TMDB update languages")
		supabase_languages = execute_sql_command("SELECT iso_639_1 FROM tmdb_language", fetch_results=True)

		if not supabase_languages:
			raise Exception("Error: Unable to retrieve Supabase languages. Skipping update.")

		tmdb_languages = get_tmdb_data(f"https://api.themoviedb.org/3/configuration/languages", {})

		if not tmdb_languages or 'success' in tmdb_languages and not tmdb_languages['success']:
			raise Exception("Error: Unable to retrieve TMDB languages. Skipping update.")

		# Créer un ensemble des iso_639_1 pour chaque source
		supabase_set = {language[0] for language in supabase_languages}
		tmdb_set = {language['iso_639_1'] for language in tmdb_languages}

		# Creer un fichier CSV avec les langues de Supabase
		create_csv_file(file_name, supabase_set)

		# Identifier les différences entre les deux ensembles
		missing_in_tmdb = supabase_set - tmdb_set
		missing_in_supabase = tmdb_set - supabase_set

		# Si des langues sont en trop dans Supabase, les supprimer
		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra languages in Supabase")
			delete_command = "DELETE FROM tmdb_language WHERE iso_639_1 IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_set - missing_in_tmdb)
			supabase_set -= missing_in_tmdb
		
		# Mettre à jour les langues dans Supabase
		with psycopg2.connect(supabase_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					# Démarrez la transaction
					connection.autocommit = False

					# Construire les valeurs à insérer dans Supabase pour tmdb_language
					values_to_insert_language = [
						(language['iso_639_1'], language['name'])
						for language in tmdb_languages
						if language['iso_639_1'] in tmdb_set
					]
					# Construire les valeurs à insérer dans Supabase pour tmdb_language_translation
					values_to_insert_translation = [
						(language['iso_639_1'], 'en', language['english_name'])
						for language in tmdb_languages
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
					connection.commit()

					# Mettre à jour le fichier CSV
					create_csv_file(file_name, supabase_set | missing_in_supabase)
					supabase_set = supabase_set | missing_in_supabase

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					connection.rollback()
					print(f"Error updating TMDB language: {e}")

				finally:
					# Rétablissez le mode autocommit à True
					connection.autocommit = True

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'language')

		print("TMDB update languages COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB language: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'language')

def tmdb_update_country(current_date: datetime, file_name: str = "tmdb_country.csv"):
	try:
		print("Starting TMDB update countries")
		supabase_countries = execute_sql_command("SELECT iso_3166_1 FROM tmdb_country", fetch_results=True)

		if not supabase_countries:
			raise Exception("Error: Unable to retrieve Supabase countries. Skipping update.")

		tmdb_countries = get_tmdb_data(f"https://api.themoviedb.org/3/configuration/countries", {'language': 'fr-FR'})

		if not tmdb_countries or 'success' in tmdb_countries and not tmdb_countries['success']:
			raise Exception("Error: Unable to retrieve TMDB countries. Skipping update.")

		# Créer un ensemble des codes pour chaque source
		supabase_set = {country[0] for country in supabase_countries}
		tmdb_set = {country['iso_3166_1'] for country in tmdb_countries}

		# Identifier les différences entre les deux ensembles
		missing_in_supabase = tmdb_set - supabase_set
		missing_in_tmdb = supabase_set - tmdb_set

		# Créez un fichier CSV avec les pays de Supabase
		create_csv_file(file_name, supabase_set)

		# Si des pays sont en trop dans Supabase, les supprimer
		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra countries in Supabase")
			delete_command = "DELETE FROM tmdb_country WHERE iso_3166_1 IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_set - missing_in_tmdb)
			supabase_set -= missing_in_tmdb
		
		# Mettre à jour les pays dans Supabase
		with psycopg2.connect(supabase_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					# Démarrez la transaction
					connection.autocommit = False

					if missing_in_supabase:
						print(f"Found {len(missing_in_supabase)} countries missing in Supabase")
						# Insérer les valeurs dans Supabase pour tmdb_country en utilisant ON CONFLICT pour l'upsert
						cursor.execute("""
							INSERT INTO tmdb_country (iso_3166_1)
							VALUES %s
							ON CONFLICT (iso_3166_1) DO NOTHING
						""", (tuple(missing_in_supabase),))

					# Construire les valeurs à insérer dans Supabase pour tmdb_country_translation
					values_to_insert_translation = [
						(country['iso_3166_1'], 'en', country['english_name'])
						for country in tmdb_countries
						if country['iso_3166_1'] in tmdb_set
					] + [
						(country['iso_3166_1'], 'fr', country['native_name'])
						for country in tmdb_countries
						if country['iso_3166_1'] in tmdb_set
					]
					
					# Insérer les valeurs dans Supabase pour tmdb_country_translation en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_country_translation (iso_3166_1, iso_639_1, name)
						VALUES (%s, %s, %s)
						ON CONFLICT (iso_3166_1, iso_639_1) DO UPDATE
						SET name = EXCLUDED.name
					""", values_to_insert_translation)

					# Valider les modifications
					connection.commit()

					# Mettre à jour le fichier CSV
					create_csv_file(file_name, supabase_set | missing_in_supabase)
					supabase_set = supabase_set | missing_in_supabase

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					connection.rollback()
					print(f"Error updating TMDB countries: {e}")

				finally:
					# Rétablissez le mode autocommit à True
					connection.autocommit = True

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'country')

		print("TMDB update countries COMPLETED")

	except Exception as e:
		print(f"Error updating TMDB countries: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'country')

# ========== END TMDB CONFIGURATION ========== #
		
# ========== START TMDB GENRE ========== #
def get_tmdb_genre_list(genre_type: str) -> dict:
	global api_key_index
	url_fr = f"https://api.themoviedb.org/3/genre/{genre_type}/list?api_key={tmdb_api_keys[api_key_index]}&language=fr-FR"
	url_en = f"https://api.themoviedb.org/3/genre/{genre_type}/list?api_key={tmdb_api_keys[api_key_index]}&language=en-US"

	response_fr = requests.get(url_fr)
	response_en = requests.get(url_en)

	genres_fr = response_fr.json()
	genres_en = response_en.json()

	# Vérifier si la clé 'success' existe et a la valeur False dans 'english' ou 'french'
	if ('success' in genres_en and not genres_en['success']) or ('success' in genres_fr and not genres_fr['success']):
		print(f"La récupération des genres de type {genre_type} a échoué.")
		return None

	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	return {"french": genres_fr['genres'], "english": genres_en['genres']}


def tmdb_update_genre(current_date: datetime, file_name: str = "tmdb_genre.csv"):
	try:
		print("Starting TMDB update genres")

		supabase_genres = execute_sql_command("SELECT id FROM tmdb_genre", fetch_results=True)

		if not supabase_genres:
			raise Exception("Error: Unable to retrieve Supabase genres. Skipping update.")
		
		# Récupérer les genres de films
		tmdb_movie_genres = get_tmdb_genre_list("movie")
		
		# Récupérer les genres de séries TV
		tmdb_tv_genres = get_tmdb_genre_list("tv")
		
		if not tmdb_movie_genres or not tmdb_tv_genres:
			raise Exception("Error: Unable to retrieve TMDB genres. Skipping update.")
		
		# Créer un ensemble des ids pour chaque source
		supabase_set = {genre[0] for genre in supabase_genres}
		tmdb_set = {genre['id'] for genre in tmdb_movie_genres["english"] + tmdb_tv_genres["english"]}

		# Créer un fichier CSV avec les genres
		create_csv_file(file_name, supabase_set)

		missing_in_supabase = tmdb_set - supabase_set
		missing_in_tmdb = supabase_set - tmdb_set

		# Si des genres sont en trop dans Supabase, les supprimer
		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra genres in Supabase")
			delete_command = "DELETE FROM tmdb_genre WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_set - missing_in_tmdb)
			supabase_set -= missing_in_tmdb
		
		if missing_in_supabase:
			print(f"Found {len(missing_in_supabase)} genres missing in Supabase")
		
		# Mettre à jour les genres dans Supabase
		with psycopg2.connect(supabase_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					# Démarrez la transaction
					connection.autocommit = False

					# Construire les valeurs à insérer dans Supabase pour tmdb_genre
					values_to_insert_genre = [
						(genre['id'],) for genre in tmdb_movie_genres["english"] + tmdb_tv_genres["english"]
					]

					# Insérer les valeurs dans Supabase pour tmdb_genre en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_genre (id)
						VALUES (%s)
						ON CONFLICT (id) DO NOTHING
					""", values_to_insert_genre)

					# Construire les valeurs à insérer dans Supabase pour tmdb_genre_translation
					values_to_insert_translation = [
						(genre['id'], 'en', genre['name']) for genre in tmdb_movie_genres["english"] + tmdb_tv_genres["english"]
					] + [
						(genre['id'], 'fr', genre['name']) for genre in tmdb_movie_genres["french"] + tmdb_tv_genres["french"]
					]

					# Insérer les valeurs dans Supabase pour tmdb_genre_translation en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_genre_translation (genre, language, name)
						VALUES (%s, %s, %s)
						ON CONFLICT (genre, language) DO UPDATE
						SET name = EXCLUDED.name
					""", values_to_insert_translation)

					# Valider les modifications
					connection.commit()

					print("TMDB update genres COMPLETED")

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					connection.rollback()
					print(f"Error updating TMDB genres: {e}")

				finally:
					# Rétablissez le mode autocommit à True
					connection.autocommit = True

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'genre')
	
	except Exception as e:
		print(f"Error updating TMDB genres: {e}")
		
		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'genre')
# ========== END TMDB GENRE ========== #
		
# ========== START TMDB KEYWORD ========== #
def tmdb_update_keyword(current_date: datetime, file_name: str = "tmdb_keyword.csv"):
	try:
		print("Starting TMDB update keywords")
		tmdb_keywords_export = get_tmdb_export_ids(type="keyword", date=current_date)
		if not tmdb_keywords_export:
			raise Exception("Error: Unable to retrieve TMDB keyword. Skipping update.")

		supabase_keywords = execute_sql_command("SELECT id FROM tmdb_keyword", fetch_results=True)
		if not supabase_keywords:
			raise Exception("Error: Unable to retrieve Supabase keywords. Skipping update.")

		# Lire le fichier JSON
		with open(tmdb_keywords_export, 'r', encoding='utf-8') as file:
			tmdb_keywords = [json.loads(line) for line in file]

		# Extraire les IDs des collections du fichier JSON
		tmdb_ids_set = {keyword['id'] for keyword in tmdb_keywords}

		# Extraire les IDs des collections dans Supabase
		supabase_ids_set = {row[0] for row in supabase_keywords}

		# Identifier les différences
		missing_in_supabase = tmdb_ids_set - supabase_ids_set
		missing_in_tmdb = supabase_ids_set - tmdb_ids_set

		# Créez un fichier CSV avec les keywords de Supabase
		create_csv_file(file_name, supabase_ids_set)

		# Si des keywords sont en trop dans Supabase, les supprimer
		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra keywords in Supabase")
			delete_command = "DELETE FROM tmdb_keyword WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_ids_set - missing_in_tmdb)
			supabase_ids_set -= missing_in_tmdb
		
		# Si des keywords sont manquants dans Supabase, les ajouter
		if missing_in_supabase:
			print(f"Found {len(missing_in_supabase)} keyword missing in Supabase")
			with psycopg2.connect(supabase_connection_string) as connection:
				with connection.cursor() as cursor:
					try:
						# Démarrez la transaction
						connection.autocommit = False

						# Construire les valeurs à insérer dans Supabase pour tmdb_keyword
						values_to_insert_keyword = [
							(keyword['id'], keyword['name'])
							for keyword in tmdb_keywords
							if keyword['id'] in missing_in_supabase
						]

						# Insérer les valeurs dans Supabase pour tmdb_keyword en utilisant ON CONFLICT pour l'upsert
						cursor.executemany("""
							INSERT INTO tmdb_keyword (id, name)
							VALUES (%s, %s)
							ON CONFLICT (id) DO UPDATE
							SET name = EXCLUDED.name
						""", values_to_insert_keyword)

						# Valider les modifications
						connection.commit()

						# Mettre à jour le fichier CSV
						create_csv_file(file_name, supabase_ids_set | missing_in_supabase)
						supabase_ids_set = supabase_ids_set | missing_in_supabase

					except Exception as e:
						# En cas d'erreur, annulez la transaction
						connection.rollback()
						print(f"Error updating TMDB keyword: {e}")

					finally:
						# Rétablissez le mode autocommit à True
						connection.autocommit = True

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'keyword')

		print("TMDB update keywords COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB keywords: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'keyword')
	finally:
		if os.path.exists(tmdb_keywords_export):
			os.remove(tmdb_keywords_export)
# ========== END TMDB KEYWORD ========== #
		
# ========== START TMDB COLLECTION ========== #
def get_tmdb_collection_details(collection_id: int) -> dict:
	global api_key_index
	url_fr = f"https://api.themoviedb.org/3/collection/{collection_id}?api_key={tmdb_api_keys[api_key_index]}&language=fr-FR"
	url_en = f"https://api.themoviedb.org/3/collection/{collection_id}?api_key={tmdb_api_keys[api_key_index]}&language=en-US"

	response_fr = requests.get(url_fr)
	response_en = requests.get(url_en)

	details_fr = response_fr.json()
	details_en = response_en.json()

	# Vérifier si la clé 'success' existe et a la valeur False dans 'english' ou 'french'
	if ('success' in details_en and not details_en['success']) or ('success' in details_fr and not details_fr['success']):
		print(f"La récupération des détails de la collection {collection_id} a échoué.")
		return None

	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	return {"french": details_fr, "english": details_en}

def tmdb_update_collection(current_date: datetime, file_name: str = "tmdb_collection.csv"):
	try:
		print("Starting TMDB update collection")
		count_added = 0
		count_deleted = 0
		tmdb_collection_export = get_tmdb_export_ids(type="collection", date=current_date)
		if not tmdb_collection_export:
			raise Exception("Error: Unable to retrieve TMDB collections. Skipping update.")
		
		supabase_collections = execute_sql_command("SELECT id FROM tmdb_collection", fetch_results=True)
		if not supabase_collections:
			raise Exception("Error: Unable to retrieve Supabase collections. Skipping update.")

		# Lire le fichier JSON
		with open(tmdb_collection_export, 'r', encoding='utf-8') as file:
			tmdb_collections = [json.loads(line) for line in file]

		# Extraire les IDs des collections du fichier JSON
		tmdb_ids_set = {collection['id'] for collection in tmdb_collections}

		# Extraire les IDs des collections dans Supabase
		supabase_ids_set = {row[0] for row in supabase_collections}

		# Identifier les différences
		missing_in_supabase = tmdb_ids_set - supabase_ids_set
		missing_in_tmdb = supabase_ids_set - tmdb_ids_set

		count_deleted = len(missing_in_tmdb)
		count_added = len(missing_in_supabase)

		# Créez un fichier CSV avec les collections de Supabase
		create_csv_file(file_name, supabase_ids_set)

		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra collections in Supabase")
			delete_command = "DELETE FROM tmdb_collection WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_ids_set - missing_in_tmdb)
			supabase_ids_set -= missing_in_tmdb
		
		if missing_in_supabase:
			chunks = list(chunked(missing_in_supabase, 500))

			print(f"Found {len(missing_in_supabase)} collections missing in Supabase")
		
			for chunk in chunks:
				# Créer une liste pour stocker les détails des personnes
				current_collections_to_update = []

				# Use ThreadPoolExecutor to execute tasks in parallel
				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = [executor.submit(get_tmdb_collection_details, collection_id) for collection_id in chunk]

					for future in futures:
						collection_details = future.result()
						if collection_details is not None:
							current_collections_to_update.append(collection_details)

				# Mettre à jour les collections dans Supabase
				with psycopg2.connect(supabase_connection_string) as connection:
					with connection.cursor() as cursor:
						try:
							# Démarrez la transaction
							connection.autocommit = False

							# Construire les valeurs à insérer dans Supabase pour tmdb_collection
							values_to_insert_collection = [
								{
									'id': collection_data['english']['id'],
									'backdrop_path': collection_data['english'].get('backdrop_path', None),
								}
								for collection_data in current_collections_to_update
							]
							# Insérer les valeurs dans Supabase pour tmdb_collection en utilisant ON CONFLICT pour l'upsert
							cursor.executemany("""
								INSERT INTO tmdb_collection (id, backdrop_path)
								VALUES (%(id)s, %(backdrop_path)s)
								ON CONFLICT (id) DO NOTHING
							""", values_to_insert_collection)

							# Construire les valeurs à insérer dans Supabase pour tmdb_collection_translations
							values_to_insert_translations = [
								{
									'collection': collection_data['english']['id'],
									'language': 'en',
									'overview': collection_data['english'].get('overview', None),
									'poster_path': collection_data['english'].get('poster_path', None),
									'name': collection_data['english'].get('name', None),
								}
								for collection_data in current_collections_to_update
							] + [
								{
									'collection': collection_data['french']['id'],
									'language': 'fr',
									'overview': collection_data['french'].get('overview', None),
									'poster_path': collection_data['french'].get('poster_path', None),
									'name': collection_data['french'].get('name', None),
								}
								for collection_data in current_collections_to_update
							]
							
							# Insérer les valeurs dans Supabase pour tmdb_collection_translations en utilisant ON CONFLICT pour l'upsert
							cursor.executemany("""
								INSERT INTO tmdb_collection_translation (collection, language, overview, poster_path, name)
								VALUES (%(collection)s, %(language)s, %(overview)s, %(poster_path)s, %(name)s)
								ON CONFLICT (collection, language) DO UPDATE
								SET overview = EXCLUDED.overview,
									poster_path = EXCLUDED.poster_path,
									name = EXCLUDED.name
							""", values_to_insert_translations)

							# Valider les modifications
							connection.commit()

							# Mettre à jour le fichier CSV
							chunk_set = set(chunk)
							create_csv_file(file_name, supabase_ids_set | chunk_set)
							supabase_ids_set = supabase_ids_set | chunk_set

						except Exception as e:
							# En cas d'erreur, annulez la transaction
							connection.rollback()
							print(f"Error updating TMDB collections: {e}")

						finally:
							# Rétablissez le mode autocommit à True
							connection.autocommit = True
		
		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'collection')

		print(f"TMDB update collections (added: {count_added}, deleted: {count_deleted}) COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB collections: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'collection')
	finally:
		if os.path.exists(tmdb_collection_export):
			os.remove(tmdb_collection_export)
# ========== END TMDB COLLECTION ========== #

# ========== START TMDB COMPANY ========== #
def get_tmdb_company_details(company_id: int) -> dict:
	global api_key_index
	url = f"https://api.themoviedb.org/3/company/{company_id}?api_key={tmdb_api_keys[api_key_index]}"

	response = requests.get(url)

	details = response.json()

	# Vérifier si la clé 'success' existe et a la valeur False dans 'english' ou 'french'
	if ('success' in details and not details['success']):
		print(f"La récupération des détails de la company {company_id} a échoué.")
		return None

	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	return details

def tmdb_update_company(current_date: datetime, file_name: str = "tmdb_company.csv"):
	try:
		print("Starting TMDB update companies")
		count_added = 0
		count_deleted = 0
		tmdb_company_export = get_tmdb_export_ids(type="production_company", date=current_date)
		if not tmdb_company_export:
			raise Exception("Error: Unable to retrieve TMDB companies. Skipping update.")
		
		supabase_companies = execute_sql_command("SELECT id FROM tmdb_company", fetch_results=True)
		if not supabase_companies:
			raise Exception("Error: Unable to retrieve Supabase companies. Skipping update.")

		# Lire le fichier JSON
		with open(tmdb_company_export, 'r', encoding='utf-8') as file:
			tmdb_companies = [json.loads(line) for line in file]

		# Extraire les IDs des companies du fichier JSON
		tmdb_ids_set = {company['id'] for company in tmdb_companies}

		# Extraire les IDs des companies dans Supabase
		supabase_ids_set = {row[0] for row in supabase_companies}

		# Identifier les différences
		missing_in_supabase = tmdb_ids_set - supabase_ids_set
		missing_in_tmdb = supabase_ids_set - tmdb_ids_set

		count_deleted = len(missing_in_tmdb)
		count_added = len(missing_in_supabase)

		# Créez un fichier CSV avec les companies de Supabase
		create_csv_file(file_name, supabase_ids_set)

		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra companies in Supabase")
			delete_command = "DELETE FROM tmdb_company WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_ids_set - missing_in_tmdb)
			supabase_ids_set -= missing_in_tmdb
		
		if missing_in_supabase:
			chunks = list(chunked(missing_in_supabase, 500))

			print(f"Found {len(missing_in_supabase)} companies missing in Supabase")
		
			for chunk in chunks:
				# Créer une liste pour stocker les détails des personnes
				current_companies_to_update = []

				# Use ThreadPoolExecutor to execute tasks in parallel
				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = [executor.submit(get_tmdb_company_details, company_id) for company_id in chunk]

					for future in futures:
						company_details = future.result()
						if company_details is not None:
							current_companies_to_update.append(company_details)

				print(f"Found {len(current_companies_to_update)} companies to update")
			 	# Mettre à jour les companies dans Supabase
				with psycopg2.connect(supabase_connection_string) as connection:
					with connection.cursor() as cursor:
						try:
							# Démarrez la transaction
							connection.autocommit = False

							# Construire les valeurs à insérer dans Supabase pour tmdb_company
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
								for company_data in current_companies_to_update
							]
							# Insérer les valeurs dans Supabase pour tmdb_company en utilisant ON CONFLICT pour l'upsert
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

							# Valider les modifications
							connection.commit()

							# Mettre à jour le fichier CSV
							chunk_set = set(chunk)
							create_csv_file(file_name, supabase_ids_set | chunk_set)
							supabase_ids_set = supabase_ids_set | chunk_set

						except Exception as e:
							# En cas d'erreur, annulez la transaction
							connection.rollback()
							print(f"Error updating TMDB companies: {e}")

						finally:
							# Rétablissez le mode autocommit à True
							connection.autocommit = True
		
		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'company')

		print(f"TMDB update companies (added: {count_added}, deleted: {count_deleted}) COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB companies: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'company')
	finally:
		if os.path.exists(tmdb_company_export):
			os.remove(tmdb_company_export)
# ========== END TMDB COMPANY ========== #

# ========== START TMDB PERSON ========== #
def get_tmdb_person_details(person_id: int) -> dict:
	global api_key_index
	url_fr = f"https://api.themoviedb.org/3/person/{person_id}?api_key={tmdb_api_keys[api_key_index]}&language=fr-FR"
	url_en = f"https://api.themoviedb.org/3/person/{person_id}?api_key={tmdb_api_keys[api_key_index]}&language=en-US"
	
	response_en = requests.get(url_en)
	response_fr = requests.get(url_fr)

	details_en = response_en.json()
	details_fr = response_fr.json()

	# Vérifier si la clé 'success' existe et a la valeur False dans 'english' ou 'french'
	if ('success' in details_en and not details_en['success']) or ('success' in details_fr and not details_fr['success']):
		print(f"La récupération des détails de la person {person_id} a échoué.")
		return None

	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	return {"french": details_fr, "english": details_en}

def update_supabase_tmdb_person(persons_to_update: list):
	try:
		with psycopg2.connect(supabase_connection_string) as connection:
			with connection.cursor() as cursor:
				try:
					# Démarrez la transaction
					connection.autocommit = False

					# Construire les valeurs à insérer dans Supabase pour tmdb_person
					values_to_insert_person = [
						{
							'id': person_data['english']['id'],
							'adult': person_data['english'].get('adult', False),
							'also_known_as': person_data['english'].get('also_known_as', []),
							'birthday': person_data['english'].get('birthday', None),
							'deathday': person_data['english'].get('deathday', None),
							'gender': person_data['english'].get('gender', None),
							'homepage': person_data['english'].get('homepage', None),
							'imdb_id': person_data['english'].get('imdb_id', None),
							'known_for_department': person_data['english'].get('known_for_department', None),
							'name': person_data['english'].get('name', None),
							'place_of_birth': person_data['english'].get('place_of_birth', None),
							'popularity': person_data['english'].get('popularity', None),
							'profile_path': person_data['english'].get('profile_path', None),
						}
						for person_data in persons_to_update
					]
					# Insérer les valeurs dans Supabase pour tmdb_person en utilisant ON CONFLICT pour l'upsert
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

					# Construire les valeurs à insérer dans Supabase pour tmdb_person_translation
					values_to_insert_person_translations = [
						{
							'person': person_data['english']['id'],
							'language': 'en',
							'biography': person_data['english'].get('biography', None),
						}
						for person_data in persons_to_update
					] + [
						{
							'person': person_data['french']['id'],
							'language': 'fr',
							'biography': person_data['french'].get('biography', None),
						}
						for person_data in persons_to_update
					]

					# Insérer les valeurs dans Supabase pour tmdb_person_translation en utilisant ON CONFLICT pour l'upsert
					cursor.executemany("""
						INSERT INTO tmdb_person_translation (person, language, biography)
						VALUES (%(person)s, %(language)s, %(biography)s)
						ON CONFLICT (person, language) DO UPDATE
						SET biography = EXCLUDED.biography
					""", values_to_insert_person_translations)

					# Valider les modifications
					connection.commit()

				except Exception as e:
					# En cas d'erreur, annulez la transaction
					connection.rollback()
					print(f"Error uploading TMDB persons in Supabase: {e}")

				finally:
					# Rétablissez le mode autocommit à True
					connection.autocommit = True
	except Exception as e:
		print(f"Error uploading TMDB persons in Supabase: {e}")

def tmdb_update_person_with_daily_export(current_date: datetime, file_name: str = "tmdb_person.csv") :
	try:
		print("Starting with TMDB Daily Export")
		count_added = 0
		count_deleted = 0
		tmdb_person_export = get_tmdb_export_ids(type="person", date=current_date)
		if not tmdb_person_export:
			raise Exception("Error: Unable to retrieve TMDB persons. Skipping update.")
		
		supabase_persons = execute_sql_command("SELECT id FROM tmdb_person", fetch_results=True)
		if not supabase_persons:
			raise Exception("Error: Unable to retrieve Supabase persons. Skipping update.")

		# Lire le fichier JSON
		with open(tmdb_person_export, 'r', encoding='utf-8') as file:
			tmdb_persons = [json.loads(line) for line in file]

		# Extraire les IDs des persons du fichier JSON
		tmdb_ids_set = {person['id'] for person in tmdb_persons}

		# Extraire les IDs des persons dans Supabase
		supabase_ids_set = {row[0] for row in supabase_persons}

		# Identifier les différences
		missing_in_supabase = tmdb_ids_set - supabase_ids_set
		missing_in_tmdb = supabase_ids_set - tmdb_ids_set

		count_deleted = len(missing_in_tmdb)
		count_added = len(missing_in_supabase)

		# Créez un fichier CSV avec les persons de Supabase
		create_csv_file(file_name, supabase_ids_set)

		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra persons in Supabase")
			delete_command = "DELETE FROM tmdb_person WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
			# Mettre à jour le fichier CSV
			create_csv_file(file_name, supabase_ids_set - missing_in_tmdb)
			supabase_ids_set -= missing_in_tmdb
		
		if missing_in_supabase:
			chunks = list(chunked(missing_in_supabase, 500))

			print(f"Found {len(missing_in_supabase)} persons missing in Supabase")
		
			for chunk in chunks:
				# Créer une liste pour stocker les détails des personnes
				current_persons_to_update = []

				# Use ThreadPoolExecutor to execute tasks in parallel
				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = [executor.submit(get_tmdb_person_details, person_id) for person_id in chunk]

					for future in futures:
						person_details = future.result()
						if person_details is not None:
							current_persons_to_update.append(person_details)
		
				print(f"Found {len(current_persons_to_update)} persons to update")
			 	# Mettre à jour les persons dans Supabase
				update_supabase_tmdb_person(current_persons_to_update)

				# Mettre à jour le fichier CSV
				chunk_set = set(chunk)
				create_csv_file(file_name, supabase_ids_set | chunk_set)
				supabase_ids_set = supabase_ids_set | chunk_set

		print(f"TMDB update with TMDB Daily Export (added: {count_added}, deleted: {count_deleted}) COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB persons with TMDB Daily Export: {e}")
	finally:
		if os.path.exists(tmdb_person_export):
			os.remove(tmdb_person_export)

def tmdb_update_person_with_changes_list(current_date: datetime, file_name: str = "tmdb_person.csv"):
	try:
		print("Starting with TMDB Changes List")
		count_updated = 0
		last_update = execute_sql_command("SELECT MAX(date) FROM tmdb_update_logs WHERE type = 'person' AND success = TRUE", fetch_results=True)[0][0]

		current_page = 1
		persons_to_update = []
		supabase_ids_set = load_csv_file(file_name)

		print(f"Curent date: {current_date}")
		print(f"Last update: {last_update}")

		while True:
			# Récupérer les changements de personne pour la page actuelle
			changed_persons_response = get_tmdb_data(url=f"https://api.themoviedb.org/3/person/changes", params={"page": current_page, "start_date": last_update, "end_date": current_date.strftime("%Y-%m-%d")})
			if not changed_persons_response:
				raise Exception("Error: Unable to retrieve TMDB changed persons. Skipping update.")
			
			changed_persons = changed_persons_response["results"]
			if not changed_persons or not len(changed_persons):
				break

			count_updated += len(changed_persons)

			# Use ThreadPoolExecutor to execute tasks in parallel
			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				futures = [executor.submit(get_tmdb_person_details, person['id']) for person in changed_persons]

				for future in futures:
					person_details = future.result()
					if person_details is not None:
						persons_to_update.append(person_details)
			
			if current_page % 2 == 0:
				# Batch update every 2 pages
				if len(persons_to_update):
					update_supabase_tmdb_person(persons_to_update)
					# Mettre à jour le fichier CSV
					chunk_set = set([person['english']['id'] for person in persons_to_update])
					create_csv_file(file_name, supabase_ids_set | chunk_set)
					supabase_ids_set = supabase_ids_set | chunk_set

				persons_to_update = []
			
			current_page += 1

		if len(persons_to_update):
			update_supabase_tmdb_person(persons_to_update)
			# Mettre à jour le fichier CSV
			chunk_set = set([person['english']['id'] for person in persons_to_update])
			create_csv_file(file_name, supabase_ids_set | chunk_set)
			supabase_ids_set = supabase_ids_set | chunk_set

		print(f"TMDB update with TMDB Changes List (updated: {count_updated}) COMPLETED")
	except Exception as e:
		print(f"Error updating TMDB persons with Changes List: {e}")

def tmdb_update_person(current_date: datetime, file_name: str = "tmdb_person.csv"):
	try:
		print("Starting TMDB update persons")

		tmdb_update_person_with_daily_export(current_date, file_name=file_name)
		tmdb_update_person_with_changes_list(current_date, file_name=file_name)

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'person')

		print("TMDB update persons COMPLETED")
	
	except Exception as e:
		print(f"Error updating TMDB persons: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'person')
# ========== END TMDB PERSON ========== #

# ========== START TMDB MOVIE ========== #
def get_tmdb_movie_details(movie_id: int) -> dict:
	global api_key_index
	url_fr = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={tmdb_api_keys[api_key_index]}&language=fr-FR&append_to_response=credits,keywords,videos,belongs_to_collection"
	url_en = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={tmdb_api_keys[api_key_index]}&language=en-US&append_to_response=credits,keywords,videos,belongs_to_collection"
	
	response_en = requests.get(url_en)
	response_fr = requests.get(url_fr)

	details_en = response_en.json()
	details_fr = response_fr.json()

	# Vérifier si la clé 'success' existe et a la valeur False dans 'english' ou 'french'
	if ('success' in details_en and not details_en['success']) or ('success' in details_fr and not details_fr['success']):
		print(f"La récupération des détails du film {movie_id} a échoué.")
		return None

	api_key_index = (api_key_index + 1) % len(tmdb_api_keys)

	return {"french": details_fr, "english": details_en}

def update_supabase_tmdb_movie(movies_to_update: list):
	try:
		csv_data = {}
		csv_data['language']= load_csv_file('tmdb_language.csv')
		csv_data['country'] = load_csv_file('tmdb_country.csv')
		csv_data['genre'] = load_csv_file('tmdb_genre.csv')
		csv_data['keyword'] = load_csv_file('tmdb_keyword.csv')
		csv_data['collection'] = load_csv_file('tmdb_collection.csv')
		csv_data['company'] = load_csv_file('tmdb_company.csv')
		csv_data['person'] = load_csv_file('tmdb_person.csv')

		with psycopg2.connect(supabase_connection_string) as connection:
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

def tmdb_update_movie_with_daily_export(current_date: datetime):
	try:
		print("Starting with TMDB Daily Export for Movies")
		count_added = 0
		count_deleted = 0
		tmdb_movie_export = get_tmdb_export_ids(type="movie", date=current_date)
		
		if not tmdb_movie_export:
			raise Exception("Error: Unable to retrieve TMDB movies. Skipping update.")
		
		supabase_movies = execute_sql_command("SELECT id FROM tmdb_movie", fetch_results=True)
		
		if not supabase_movies:
			raise Exception("Error: Unable to retrieve Supabase movies. Skipping update.")

		# Lire le fichier JSON
		with open(tmdb_movie_export, 'r', encoding='utf-8') as file:
			tmdb_movies = [json.loads(line) for line in file]

		# Extraire les IDs des films du fichier JSON
		tmdb_ids_set = {movie['id'] for movie in tmdb_movies}

		# Extraire les IDs des films dans Supabase
		supabase_ids_set = {row[0] for row in supabase_movies}

		# Identifier les différences
		missing_in_supabase = tmdb_ids_set - supabase_ids_set
		missing_in_tmdb = supabase_ids_set - tmdb_ids_set

		count_deleted = len(missing_in_tmdb)
		count_added = len(missing_in_supabase)

		if missing_in_tmdb:
			print(f"Found {len(missing_in_tmdb)} extra movies in Supabase")
			delete_command = "DELETE FROM tmdb_movie WHERE id IN %s"
			execute_sql_command(delete_command, (tuple(missing_in_tmdb),))
		
		if missing_in_supabase:
			chunks = list(chunked(missing_in_supabase, 500))

			print(f"Found {len(missing_in_supabase)} movies missing in Supabase")

			for chunk in chunks:
				# Créer une liste pour stocker les détails des films
				current_movies_to_update = []

				# Use ThreadPoolExecutor to execute tasks in parallel
				with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
					futures = [executor.submit(get_tmdb_movie_details, movie_id) for movie_id in chunk]

					for future in futures:
						movie_details = future.result()
						if movie_details is not None:
							current_movies_to_update.append(movie_details)

				print(f"Found {len(current_movies_to_update)} movies to update")

				# Mettre à jour les films dans Supabase
				update_supabase_tmdb_movie(current_movies_to_update)

		print(f"TMDB update with TMDB Daily Export for Movies (added: {count_added}, deleted: {count_deleted}) COMPLETED")
	except Exception as e:
		print(f"Error updating TMDB movies with TMDB Daily Export: {e}")
	finally:
		if os.path.exists(tmdb_movie_export):
			os.remove(tmdb_movie_export)

def tmdb_update_movie_with_changes_list(current_date: datetime):
	try:
		print("Starting with TMDB Changes List for Movies")
		count_updated = 0
		last_update = execute_sql_command("SELECT MAX(date) FROM tmdb_update_logs WHERE type = 'movie' AND success = TRUE", fetch_results=True)[0][0]

		current_page = 1
		movies_to_update = []

		print(f"Current date: {current_date}")
		print(f"Last update: {last_update}")

		while True:
			# Récupérer les changements de film pour la page actuelle
			changed_movies_response = get_tmdb_data(url=f"https://api.themoviedb.org/3/movie/changes", params={"page": current_page, "start_date": last_update, "end_date": current_date.strftime("%Y-%m-%d")})
			
			if not changed_movies_response:
				raise Exception("Error: Unable to retrieve TMDB changed movies. Skipping update.")
			
			changed_movies = changed_movies_response["results"]
			
			if not changed_movies or not len(changed_movies):
				break

			count_updated += len(changed_movies)

			# Use ThreadPoolExecutor to execute tasks in parallel
			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				futures = [executor.submit(get_tmdb_movie_details, movie['id']) for movie in changed_movies]

				for future in futures:
					movie_details = future.result()
					if movie_details is not None:
						movies_to_update.append(movie_details)
			
			if current_page % 2 == 0:
				# Mettre à jour par lot tous les 2 pages
				if len(movies_to_update):
					update_supabase_tmdb_movie(movies_to_update)
				movies_to_update = []

			current_page += 1

		if len(movies_to_update):
			update_supabase_tmdb_movie(movies_to_update)

		print(f"TMDB update with TMDB Changes List for Movies (updated: {count_updated}) COMPLETED")

	except Exception as e:
		print(f"Error updating TMDB movies with Changes List: {e}")


def tmdb_update_movie(current_date: datetime):
	try:
		print("Starting TMDB update movies")

		tmdb_update_movie_with_daily_export(current_date)
		tmdb_update_movie_with_changes_list(current_date)

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, True, 'movie')

		print("TMDB update movies COMPLETED")
	except Exception as e:
		print(f"Error updating TMDB movies: {e}")

		# Ajouter un log dans la table supabase tmdb_update_logs
		supabase_tmdb_update_log(current_date, False, 'movie')
# ========== END TMDB MOVIE ========== #


def tmdb_update():
	print("Starting TMDB update with script VERSION 1.0.0")
	current_date = datetime.now()
	try:
		tmdb_update_language(current_date=current_date)
		tmdb_update_country(current_date=current_date)
		tmdb_update_genre(current_date=current_date)
		tmdb_update_keyword(current_date=current_date)
		tmdb_update_collection(current_date=current_date)
		tmdb_update_company(current_date=current_date)
		tmdb_update_person(current_date=current_date)
		tmdb_update_movie(current_date=current_date)
	except Exception as e:
		print(f"TMDB update failed: {e}")
		raise
	finally:
		if os.path.exists("tmdb_language.csv"):
			os.remove("tmdb_language.csv")
		if os.path.exists("tmdb_country.csv"):
			os.remove("tmdb_country.csv")
		if os.path.exists("tmdb_genre.csv"):
			os.remove("tmdb_genre.csv")
		if os.path.exists("tmdb_keyword.csv"):
			os.remove("tmdb_keyword.csv")
		if os.path.exists("tmdb_collection.csv"):
			os.remove("tmdb_collection.csv")
		if os.path.exists("tmdb_company.csv"):
			os.remove("tmdb_company.csv")
		if os.path.exists("tmdb_person.csv"):
			os.remove("tmdb_person.csv")

if __name__ == "__main__":
	tmdb_update()

