import os.path

import duckdb
import kagglehub

#CONSTANTS
AV_THREADS:int = 2
KAGGLE_LINK:str = 'mhassansaboor/alibaba-stock-dataset-2025'

source_path = ''

def download_csv():
    global source_path
    try:
        print("Downloading CSV...")
        source_path = kagglehub.dataset_download(KAGGLE_LINK)
        print(f"Download complete. {source_path}")
    except Exception as e:
        print(e)

def create_db(db_name: str):
    try:
        db = duckdb.connect(db_name, config={'threads': AV_THREADS})
    except Exception as e:
        print(e)
    finally:
        return db

if __name__ == '__main__':
    try:
        download_csv()
        connection = create_db('dwh.db')
        f_path:str = ''
        with os.scandir(source_path) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith('.csv'):
                    f_path = os.path.join(source_path, entry.name)
                    connection.read_csv(f_path).write_parquet(entry.name[:-4] + '.parquet')
                    connection.read_parquet(entry.name[:-4] + '.parquet')

    except Exception as e:
        print(e)