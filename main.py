import duckdb

def create_db(db_name: str):
    try:
        db = duckdb.connect(db_name, config={'threads': 2})
    except Exception as e:
        print(e)
    finally:
        return db

if __name__ == '__main__':
    connection = create_db('dwh.db')