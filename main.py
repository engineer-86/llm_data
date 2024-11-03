import pandas as pd
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm
import os
import json
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

data_folder ="data_llm"
load_dotenv()


MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_DB = os.getenv('MONGO_AUTH_DB', 'admin')
MONGO_MAX_POOL_SIZE = int(os.getenv('MONGO_MAX_POOL_SIZE', 100))


def connect_to_mongodb(host=MONGO_HOST, port=MONGO_PORT, username=MONGO_USERNAME, password=MONGO_PASSWORD,
                       auth_db=MONGO_AUTH_DB, max_pool_size=MONGO_MAX_POOL_SIZE):

    if username and password:
        mongo_client = MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource=auth_db,
            maxPoolSize=max_pool_size
        )
    else:
        mongo_client = MongoClient(
            host=host,
            port=port,
            maxPoolSize=max_pool_size
        )
    print(f"Verbindung zu MongoDB bei {host}:{port} hergestellt (max. Poolgröße: {max_pool_size}).")
    return mongo_client



client = connect_to_mongodb()
db = client['imdb_database']


progress_file = 'import_progress.json'



def save_progress(file_name, last_index):
    with open(progress_file, 'w') as f:
        json.dump({'file_name': file_name, 'last_index': last_index}, f)


# Funktion zum Laden des Fortschritts
def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return json.load(f)
    return None



files = [
    ('title.akas.tsv', 'title_akas'),
    ('title.basics.tsv', 'title_basics'),
    ('title.crew.tsv', 'title_crew'),
    ('title.ratings.tsv', 'title_ratings'),
    ('title.episode.tsv', 'title_episodes'),
    ('name.basics.tsv', 'name_basics'),
    ('title.principals.tsv', 'title_principals')
]



def import_record_parallel(record, collection_name):
    db[collection_name].insert_one(record)



progress = load_progress()

for file_name, collection_name in files:
    try:
        if progress and progress['file_name'] == file_name:
            start_index = progress['last_index']
            print(f'Fortsetzen des Imports von {file_name} ab Datensatz {start_index}...')
        else:
            start_index = 0
            print(f'Einlesen der Datei {file_name}...')

        df = pd.read_csv(os.path.join(data_folder, file_name), sep='\t', na_values='\\N', low_memory=False)
        records = df.to_dict('records')


        with ThreadPoolExecutor(max_workers=8) as executor:
            for index, _ in enumerate(tqdm(executor.map(lambda record: import_record_parallel(record, collection_name),
                                                        records[start_index:]),
                                           desc=f'Import {collection_name}',
                                           total=len(records[start_index:]),
                                           unit='records')):
                save_progress(file_name, start_index + index + 1)

        print(f'Datei {file_name} erfolgreich importiert.')
        if os.path.exists(progress_file):
            os.remove(progress_file)
    except Exception as e:
        print(f'Fehler beim Einlesen oder Importieren der Datei {file_name}: {e}')
        continue


print('Erstelle Indizes...')
try:
    db.title_akas.create_index([("titleId", ASCENDING)])
    db.title_basics.create_index([("tconst", ASCENDING)])
    db.title_crew.create_index([("tconst", ASCENDING)])
    db.title_ratings.create_index([("tconst", ASCENDING)])
    db.title_episodes.create_index([("tconst", ASCENDING), ("parentTconst", ASCENDING)])
    db.name_basics.create_index([("nconst", ASCENDING)])
    db.title_principals.create_index([("tconst", ASCENDING), ("nconst", ASCENDING)])
    print("Indizes erfolgreich erstellt.")
except Exception as e:
    print(f'Fehler beim Erstellen der Indizes: {e}')

print("Prozess abgeschlossen.")
