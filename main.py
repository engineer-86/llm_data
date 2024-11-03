import dask.dataframe as dd
from pymongo import MongoClient, ASCENDING, InsertOne
from tqdm import tqdm
import os
import json
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import math

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_DB = os.getenv('MONGO_AUTH_DB', 'admin')
MONGO_MAX_POOL_SIZE = int(os.getenv('MONGO_MAX_POOL_SIZE', 100))
BATCH_SIZE = 1000
data_folder = "data_llm"

def connect_to_mongodb(host=MONGO_HOST, port=MONGO_PORT, username=MONGO_USERNAME, password=MONGO_PASSWORD, auth_db=MONGO_AUTH_DB, max_pool_size=MONGO_MAX_POOL_SIZE):
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
    print(f"Connected to MongoDB at {host}:{port} (max pool size: {max_pool_size}).")
    return mongo_client

client = connect_to_mongodb()
db = client['imdb_database']

progress_file = 'import_progress.json'

def save_progress(file_name, last_index):
    with open(progress_file, 'w') as f:
        json.dump({'file_name': file_name, 'last_index': last_index}, f)

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

# Dateien nach Dateigröße sortieren
files_sorted_by_size = sorted(
    files,
    key=lambda x: os.path.getsize(os.path.join(data_folder, x[0]))
)

custom_dtypes = {
    'title.akas.tsv': {'types': 'object'},
    'title.basics.tsv': {'runtimeMinutes': 'object', 'genres': 'object'},
    'title.crew.tsv': {'directors': 'object', 'writers': 'object'},
    'title.ratings.tsv': {'averageRating': 'float64', 'numVotes': 'int64'},
    'title.episode.tsv': {'seasonNumber': 'object', 'episodeNumber': 'object'},
    'name.basics.tsv': {'primaryProfession': 'object', 'knownForTitles': 'object'},
    'title.principals.tsv': {'category': 'object', 'job': 'object', 'characters': 'object'}
}

def import_batch_parallel(records, collection_name):
    try:
        db_collection = db[collection_name]
        requests = [InsertOne(record) for record in records]
        db_collection.bulk_write(requests, ordered=False)
    except Exception as e:
        print(f'Error in bulk write to {collection_name}: {e}')

progress = load_progress()

for file_name, collection_name in files_sorted_by_size:
    try:
        if progress and progress['file_name'] == file_name:
            start_index = progress['last_index']
            print(f'Resuming import of {file_name} from record {start_index}...')
        else:
            start_index = 0
            print(f'Reading file {file_name}...')

        dtype = custom_dtypes.get(file_name, None)
        df = dd.read_csv(os.path.join(data_folder, file_name), sep='\t', na_values='\\N', blocksize="64MB", dtype=dtype, low_memory=False)
        records = df.compute().to_dict('records')

        num_batches = math.ceil((len(records) - start_index) / BATCH_SIZE)

        with ThreadPoolExecutor(max_workers=8) as executor:
            for i in tqdm(range(num_batches), desc=f'Import {collection_name}', unit='batches'):
                batch_start = start_index + i * BATCH_SIZE
                batch_end = min(batch_start + BATCH_SIZE, len(records))
                batch = records[batch_start:batch_end]
                executor.submit(import_batch_parallel, batch, collection_name)
                save_progress(file_name, batch_end)

        print(f'File {file_name} imported successfully.')
        if os.path.exists(progress_file):
            os.remove(progress_file)
    except Exception as e:
        print(f'Error reading or importing file {file_name}: {e}')
        continue

print('Creating indexes...')
try:
    db.title_akas.create_index([("titleId", ASCENDING)])
    db.title_basics.create_index([("tconst", ASCENDING)])
    db.title_crew.create_index([("tconst", ASCENDING)])
    db.title_ratings.create_index([("tconst", ASCENDING)])
    db.title_episodes.create_index([("tconst", ASCENDING), ("parentTconst", ASCENDING)])
    db.name_basics.create_index([("nconst", ASCENDING)])
    db.title_principals.create_index([("tconst", ASCENDING), ("nconst", ASCENDING)])
    print("Indexes created successfully.")
except Exception as e:
    print(f'Error creating indexes: {e}')

print("Process completed.")
