from rich import print
from datetime import datetime, date

# Custom
from db.schemas import DBSchemas
import utils.db as db
import utils.tmdb as tmdb

def flow(date: date):
	try:
		print("Starting country flow...")

		# Get language in DB
		with db.connect() as conn:
			db_countries: list = db.get_table(conn, DBSchemas.COUNTRY, ["iso_3166_1"])
		if not db_countries:
			raise Exception("No countries found in DB")

		# Get language from TMDB
		tmdb_countries: list = tmdb.get_data("configuration/countries", {"language": "fr-FR"})
		if not tmdb_countries:
			raise Exception("No countries found in TMDB")

		# Compare languages
		db_countries_set: set = set([item[0] for item in db_countries])
		tmdb_countries_set: set = set([item["iso_3166_1"] for item in tmdb_countries])

		# Get missing and extra languages
		missing_countries: set = tmdb_countries_set - db_countries_set
		extra_countries: set = db_countries_set - tmdb_countries_set

		if extra_countries:
			print(f"Found {len(extra_countries)} extra countries")
			with db.connect() as conn:
				db.execute_query(conn, f"DELETE FROM {DBSchemas.COUNTRY} WHERE iso_3166_1 IN %s", (tuple(extra_countries),))
				print(f"Deleted {len(extra_countries)} extra countries")
			db_countries_set -= extra_countries

		with db.connect() as conn:
			with conn.cursor() as cursor:
				try:
					conn.autocommit = False
					if missing_countries:
						print(f"Found {len(missing_countries)} missing countries")
						cursor.execute(f"""
							INSERT INTO {DBSchemas.COUNTRY} (iso_3166_1)
							VALUES %s
							ON CONFLICT (iso_3166_1) DO NOTHING
						""", (tuple(missing_countries),))
					
					values_to_insert_translation = [
						(country['iso_3166_1'], 'en', country['english_name'])
						for country in tmdb_countries
						if country['iso_3166_1'] in tmdb_countries_set
					] + [
						(country['iso_3166_1'], 'fr', country['native_name'])
						for country in tmdb_countries
						if country['iso_3166_1'] in tmdb_countries_set
					]
					
					cursor.execute(f"""
						INSERT INTO {DBSchemas.COUNTRY_TRANSLATION} (iso_3166_1, iso_639_1, name)
						VALUES {','.join(['(%s, %s, %s)' for _ in values_to_insert_translation])}
						ON CONFLICT (iso_3166_1, iso_639_1) DO UPDATE
						SET name = EXCLUDED.name
					""", [item for sublist in values_to_insert_translation for item in sublist])

					conn.commit()

					db_countries_set |= missing_countries
				except Exception as e:
					conn.rollback()
					raise e
				finally:
					conn.autocommit = True
		with db.connect() as conn:
			db.insert_sync_log(conn, date, "country", True)
	except Exception as e:
		with db.connect() as conn:
			db.insert_sync_log(conn, date, "country", False)
		raise Exception(f"Country flow failed: {e}")
