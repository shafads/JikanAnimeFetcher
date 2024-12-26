import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

class DBConnection:
    def __init__(self):
        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")

    # Function to connect to the database
    def connect_to_db(self):
        try:
            conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
            print("Connection successful.")
            return conn
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            return None
    
    # Function to insert data into a table, avoiding duplicate records
    def insert_data(self, table_name, data):
        """Insert data into the database, avoiding duplicates."""
        conn = self.connect_to_db()
        if conn is None:
            return
        
        try:
            with conn.cursor() as cur:
                for index, row in data.iterrows():
                    mal_id = row['mal_id']
                    # Check if the record already exists
                    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE mal_id = %s", (mal_id,))
                    exists = cur.fetchone()[0]

                    if exists == 0:  # Only insert if it doesn't exist
                        insert_query = f"""
                        INSERT INTO {table_name} (mal_id, title, title_english,synopsis, genres, status, score,
                                                  scored_by, type, source, episodes, popularity,
                                                  members, rank, favorites, season, year)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(insert_query, tuple(row))
                # Commit the changes only once after the loop
                conn.commit()
                print("Data insertion complete.")
        except psycopg2.Error as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            conn.close()

    # Function to create a table
    def create_table(self, table_name):
        conn = self.connect_to_db()
        if conn is None:
            return
        
        try:
            with conn.cursor() as cur:
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        mal_id BIGINT PRIMARY KEY,
                        title VARCHAR(255),
                        title_english VARCHAR(255),
                        synopsis TEXT,
                        genres VARCHAR(255),
                        status VARCHAR(50),
                        score FLOAT,
                        scored_by BIGINT,
                        type VARCHAR(50),
                        source VARCHAR(50),
                        episodes BIGINT,
                        popularity BIGINT,
                        members BIGINT,
                        rank BIGINT,
                        favorites BIGINT,
                        season VARCHAR(50),
                        year BIGINT
                    );
                """
                cur.execute(create_table_query)
                # Create an index on mal_id for faster lookup
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_mal_id ON {table_name}(mal_id);")
                print(f"Table {table_name} created successfully with indexing.")
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            conn.rollback()
        finally:
            conn.close()
