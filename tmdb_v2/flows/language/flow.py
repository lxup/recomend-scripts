from rich import print
from datetime import datetime, date

# Custom
from db.schemas import DBSchemas
import utils.db as db
import utils.tmdb as tmdb

def flow(date: date):
	try:
		print("Starting language flow...")

		# Get language in DB
		with db.connect() as conn:
			db_languages: list = db.get_table(conn, DBSchemas.LANGUAGE, ["iso_639_1"])
		if not db_languages:
			raise Exception("No languages found in DB")

		# Get language from TMDB
		tmdb_languages: list = tmdb.get_data("configuration/languages", {})
		if not tmdb_languages:
			raise Exception("No languages found in TMDB")

		# Compare languages
		db_languages_set: set = set([lang[0] for lang in db_languages])
		tmdb_languages_set: set = set([lang["iso_639_1"] for lang in tmdb_languages])

		# Get missing and extra languages
		missing_languages: set = tmdb_languages_set - db_languages_set
		extra_languages: set = db_languages_set - tmdb_languages_set

		if extra_languages:
			print(f"Found {len(extra_languages)} extra languages")
			with db.connect() as conn:
				db.execute_query(conn, f"DELETE FROM {DBSchemas.LANGUAGE} WHERE iso_639_1 IN %s", (tuple(extra_languages),))
				print(f"Deleted {len(extra_languages)} extra languages")
			db_languages_set -= extra_languages

		with db.connect() as conn:
			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					if missing_languages:
						print(f"Found {len(missing_languages)} missing languages")
						values_to_insert_language = [
							(language['iso_639_1'], language['name'])
							for language in tmdb_languages
							if language['iso_639_1'] in tmdb_languages_set
						]
						cursor.execute(f"""
							INSERT INTO {DBSchemas.LANGUAGE} (iso_639_1, name_in_native_language)
							VALUES {', '.join(['(%s, %s)' for _ in values_to_insert_language])}
							ON CONFLICT (iso_639_1) DO UPDATE
							SET name_in_native_language = EXCLUDED.name_in_native_language
						""", [item for sublist in values_to_insert_language for item in sublist])
					
					values_to_insert_translation = [
						(language['iso_639_1'], 'en', language['english_name'])
						for language in tmdb_languages
						if language['iso_639_1'] in tmdb_languages_set
					]

					cursor.execute(f"""
						INSERT INTO {DBSchemas.LANGUAGE_TRANSLATION} (iso_639_1, language, name)
						VALUES {', '.join(['(%s, %s, %s)' for _ in values_to_insert_translation])}
						ON CONFLICT (iso_639_1, language) DO UPDATE
						SET name = EXCLUDED.name
					""", [item for sublist in values_to_insert_translation for item in sublist])

					conn.commit()

					db_languages_set |= missing_languages
				except Exception as e:
					conn.rollback()
					raise e
				finally:
					conn.autocommit = True
		with db.connect() as conn:
			db.insert_sync_log(conn, date, "language", True)
	except Exception as e:
		with db.connect() as conn:
			db.insert_sync_log(conn, date, "language", False)
		raise Exception(f"Language flow failed: {e}")
