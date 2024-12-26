import jikanAnimeFetcher
from jikanpy import Jikan
import dbConnection
import json
import pandas as pd

if __name__ == "__main__":
    # Define the year and seasons
    year = [2024]
    seasons = ['summer']
    jikan = Jikan()

    af = jikanAnimeFetcher.AnimeFetcher(year, seasons)

    all_anime_data = af.fetch_anime_data_multiple_seasons(years=year, seasons=seasons)
    
    af.fetch_all_character_data(all_anime_data['anime_id'])
    af.fetch_all_reviews_data(all_anime_data['anime_id'])

    af.save_to_csv("anime_data.csv", all_anime_data)
    af.save_to_csv("review_data.csv", af.extract_reviews_info())
    af.save_to_csv("cahracter_information.csv", af.extract_character_info())
    af.save_to_csv("voice_actor_information.csv", af.extract_VA_info())

    # # Initialize the DB connection class
    # db_conn = dbConnection.DBConnection()

    # # Create the table in the database
    # db_conn.create_table("anime_data")

    # # Insert the data into the table
    # db_conn.insert_data("anime_data", all_anime_data)


