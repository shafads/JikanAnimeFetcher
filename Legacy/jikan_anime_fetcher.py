from jikanpy import Jikan
import pandas as pd
import json
import psycopg2
import time
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

#anime fetcher from jikan api
class AnimeFetcher:
    def __init__(self, year, season):
        self.jikan = Jikan()
        self.year = year
        self.season = season
        self.anime_data = None

    #fetch anime data perseason for every page of that season
    def fetch_anime_data_per_season(self):
        """Fetch anime data for a specific season and year from the Jikan API."""
        page = 1
        all_anime_data = []  # List to hold all anime data from multiple pages

        while True:
            try:
                anime_data = self.jikan.seasons(year=self.year, season=self.season, page=page)
                if not anime_data['data']:
                    break  # Stop if there are no more anime data

                all_anime_data.extend(anime_data['data'])  # Collect all data

                # Add a delay of 1 seconds before fetching the next page
                time.sleep(1)

                page += 1  # Move to the next page
            except Exception as e:
                print(f"Error fetching data from page {page}: {e}")
                break  # Exit if there's an error

        self.anime_data = {'data': all_anime_data}  # Set the anime_data attribute



    def extract_anime_info(self):
        """Extract relevant anime information into a DataFrame."""
        anime_list = []
        for anime in self.anime_data['data']:
            genres = ', '.join([genre['name'] for genre in anime['genres']])
            anime_list.append({
                'mal_id': anime['mal_id'],  # Include mal_id here
                'title': anime['title'],
                'title_english': anime['title_english'],
                'genres': genres,
                'status': anime['status'],
                'score': anime['score'],
                'scored_by': anime['scored_by'],
                'type': anime['type'],
                'source': anime['source'],
                'episodes': anime['episodes'],
                'popularity': anime['popularity'],
                'members': anime['members'],
                'rank': anime['rank'],
                'favorites': anime['favorites'],
                'season': anime['season'],
                'year': anime['year']
            })
        # Create a DataFrame from the list of dictionaries
        anime_df = pd.DataFrame(anime_list)
        return anime_df

    #make a function to Fetch anime data for multiple year and season
    def fetch_anime_data_multiple_seasons(self, years, seasons):
        """Fetch anime data for multiple years and seasons and save it to CSV."""
        all_anime_data = pd.DataFrame()  # Initialize an empty DataFrame to hold all seasons' data

        for y in years:
            for s in seasons:
                print(f"Fetching data for {y} {s}...")
                self.year = y
                self.season = s
                self.fetch_anime_data_per_season()

                # Extract anime info for this season
                anime_df = self.extract_anime_info()

                # Check if anime_df is not empty before concatenating
                if not anime_df.empty:
                    all_anime_data = pd.concat([all_anime_data, anime_df], ignore_index=True)
                    
                # time.sleep(1)  # Pause for 3 seconds between requests

        # Turn NaN and null values into 0
        all_anime_data = all_anime_data.fillna(0)
        # Change the data type of specific columns from float to int
        all_anime_data = all_anime_data.astype({
            'mal_id': 'int', 'scored_by': 'int', 'episodes': 'int',
            'popularity': 'int', 'members': 'int', 'rank': 'int',
            'favorites': 'int', 'year': 'int'
        })
        # Change the 0 value to null value
        all_anime_data = all_anime_data.replace(0, None)
        return all_anime_data


    #clean data
    def clean_data(self, all_anime_data):
        """Clean the anime data."""
        # Remove duplicates
        all_anime_data = all_anime_data.drop_duplicates(subset=['mal_id'])
        # Remove rows with missing values
        all_anime_data = all_anime_data.dropna()
        return all_anime_data
    
    #create a function to save the fetched anime data to a JSON file
    def save_to_json(self, filename):
        """Save the fetched anime data to a JSON file."""
        if self.anime_data is not None:
            with open(filename,
                        'w') as file:
                    json.dump(self.anime_data, file, indent=4)
            print(f"Data has been saved to {filename}.")

    #create a fuction that turn extracted data into a csv file
    def save_to_csv(self, filename, all_anime_data):
        """Save the full extracted anime data from all seasons to a CSV file."""
        if not all_anime_data.empty:
            all_anime_data.to_csv(filename, index=False)
            print(f"All season data has been saved to {filename}.")
        else:
            print("No anime data to save.")


#database connection
class db_connection:
    # Function to connect to the database
    def connect_to_db(self, dbname, user, password, host, port):
        try:
            conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
            print("Connection successful.")
            return conn
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            return None
    
    # Function to insert data into a  table for not duplicate data
    def insert_data(self, table_name, data, dbname, user, password, host, port):
        """Insert data into the database, avoiding duplicates."""
        with self.connect_to_db(dbname, user, password, host, port) as conn:
            with conn.cursor() as cur:
                for index, row in data.iterrows():
                    mal_id = row['mal_id']
                    # Check if the record already exists
                    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE mal_id = %s", (mal_id,))
                    exists = cur.fetchone()[0]

                    if exists == 0:  # Only insert if it doesn't exist
                        insert_query = f"""
                        INSERT INTO {table_name} (mal_id, title, title_english, genres, status, score,
                                                  scored_by, type, source, episodes, popularity,
                                                  members, rank, favorites, season, year)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(insert_query, tuple(row))

            # Commit the changes
            conn.commit()
            print("Data insertion complete.")

    def create_table(self, table_name, dbname, user, password, host, port):
        with self.connect_to_db(dbname, user, password, host, port) as conn:
            with conn.cursor() as cur:
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        mal_id BIGINT,
                        title VARCHAR(255),
                        title_english VARCHAR(255),
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
                try:
                    cur.execute(create_table_query)
                    conn.commit()
                except Exception as e:
                    print("Error creating table:", e)

        # Commit the changes
        conn.commit()
        
        # Close the cursor and the connection
        cur.close()
        conn.close()
        
        print(f"Table {table_name} has been created.")


if __name__ == "__main__":
    # Define the year and seasons
    year = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]
    seasons = ['summer', 'fall', 'winter', 'spring']

    # Initialize the database connection details (replace these with actual credentials)
    db_details = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT'),
    }

    # Initialize the AnimeFetcher object
    af = AnimeFetcher(year, seasons)

    # Fetch anime data for multiple years and seasons
    all_anime_data = af.fetch_anime_data_multiple_seasons(years=year, seasons=seasons)

    # Save the combined anime data to a json file
    af.save_to_json("anime_data.json")

    # Save the combined anime data to a CSV file
    af.save_to_csv("anime_data.csv", all_anime_data)

    # Initialize the DB connection class
    db_conn = db_connection()

    # Create the table in the database
    db_conn.create_table("anime_data", **db_details)

    # Insert the data into the database
    db_conn.insert_data("anime_data", all_anime_data, **db_details)
    print("All data has been fetched, saved, and inserted into the database.")
   