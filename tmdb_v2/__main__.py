# ---------------------------------------------------------------------------- #
#                                    Library                                   #
# ---------------------------------------------------------------------------- #
import os
from dotenv import load_dotenv
from rich import print
from datetime import date

# Custom
from utils.tmdb import set_api_keys

# ---------------------------------------------------------------------------- #
#                                     Flows                                    #
# ---------------------------------------------------------------------------- #
import flows.language.flow as flow_language
import flows.country.flow as flow_country
# import flows.genre.flow as flow_genre

load_dotenv()

if __name__ == "__main__":
	
	try:
		# Check POSTGRES_CONNECTION_STRING
		if not os.getenv("POSTGRES_CONNECTION_STRING"):
			raise Exception("POSTGRES_CONNECTION_STRING is not set")
		# Check TMDB_API_KEYS
		if not os.getenv("TMDB_API_KEYS") or len(os.getenv("TMDB_API_KEYS").split(",")) < 1:
			raise Exception("TMDB_API_KEYS is not set or empty")
		set_api_keys(os.getenv("TMDB_API_KEYS"))
		
		start_time = date.today()

		print(f"Starting TMDB update on {start_time}")
	
		# Run flow
		flow_language.flow(start_time)
		flow_country.flow(start_time)
		# flow_genre.flow(start_time)
	except Exception as e:
		print(f"[red]Flow failed: {e}[/red]")
		exit(1)	