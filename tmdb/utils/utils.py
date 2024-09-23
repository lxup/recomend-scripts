import os
import csv

# Create directory if it doesn't exist
def create_directory(directory: str):
	if not os.path.exists(directory):
		os.makedirs(directory)

# Clean up list of files
def clean_files(tmp_files: list):
	for file in tmp_files:
		if os.path.exists(file):
			os.remove(file)

def create_csv_file(file_name, data, append=False, headerValue="id"):
	mode = 'a' if append else 'w'

	with open(file_name, mode, newline='', encoding='utf-8') as file:
		writer = csv.writer(file)

		# Si on crée un nouveau fichier, écrire l'en-tête
		if not append:
			header = [headerValue]
			writer.writerow(header)

		# Écrire toutes les valeurs de data
		for value in data:
			writer.writerow([value])

