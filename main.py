import duckdb
import kagglehub

#CONSTANTS
AV_THREADS = 2
KAGGLE_LINK = 'https://www.kaggle.com/datasets/mhassansaboor/alibaba-stock-dataset-2025'

def download_csv():
    print("Downloading CSV...")
    try:
        kagglehub.login()
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
    connection = create_db('dwh.db')