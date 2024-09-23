import os
import requests
from itertools import cycle

api_key_cycle = None

def set_api_keys(api_keys):
	global api_key_cycle
	api_key_cycle = cycle(api_keys.split(","))

def get_data(endpoint: str, params: dict) -> dict:
	if api_key_cycle is None:
		raise Exception("API keys not set")

	api_key = next(api_key_cycle)
	params["api_key"] = api_key

	url = f"https://api.themoviedb.org/3/{endpoint}"
	response = requests.get(url, params=params)
	if response.status_code != 200:
		return None

	data = response.json()

	if ("success" in data and not data["success"]):
		return None

	return data
