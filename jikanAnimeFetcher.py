from jikanpy import Jikan
import pandas as pd
import json
import time
from jikanpy.exceptions import APIException

class AnimeFetcher:
    def __init__(self, year, season):
        self.jikan = Jikan()
        self.year = year
        self.season = season
        self.anime_data = None
        self.character_data = None
        self.reviews_data = None

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
                print(f"Fetched data from page {page}")
                # Add a delay of 1 seconds before fetching the next page
                time.sleep(0.8)
                page += 1  # Move to the next page
            except Exception as e:
                print(f"Error fetching data from page {page}: {e}")
                break  # Exit if there's an error
            #print the total number of anime data fetched
        print(f"Total anime data fetched: {len(all_anime_data)}")
        self.anime_data = {'data': all_anime_data}  # Set the anime_data attribute

    def extract_anime_info(self):
        """Extract relevant anime information into a DataFrame."""
        anime_list = []
        for anime in self.anime_data['data']:
            genres = ', '.join([genre['name'] for genre in anime['genres']])
            anime_list.append({
                'anime_id': anime['mal_id'],
                'title': anime['title'],
                'title_english': anime['title_english'],
                'synopsis': anime['synopsis'],
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
            'anime_id': 'int', 'scored_by': 'int', 'episodes': 'int',
            'popularity': 'int', 'members': 'int', 'rank': 'int',
            'favorites': 'int', 'year': 'int'
        })
        # Change the 0 value to null value
        all_anime_data = all_anime_data.replace(0, None)
        self.anime_data = all_anime_data
        print("All season data fetched.")
        print("total data fetched: ", len(self.anime_data))
        return all_anime_data
    
    def fecth_character_data(self, mal_id):
        """Fetch character data for a specific anime using the Jikan API."""
        #create a dataframe to hold the character data
        #insert the mal_id into the dataframe
        try:
            character_data = self.jikan.anime(mal_id, extension='characters')
            time.sleep(0.8)  # Add a delay of 1 second before fetching the next character data
            return character_data['data']
        except APIException as e:
            print(f"Error fetching character data for {mal_id}: {e}")
            return None
    
    def fetch_all_character_data(self, mal_ids):
        """Fetch character data for multiple anime using the Jikan API."""
        all_character_data = pd.DataFrame()  # Initialize an empty DataFrame to hold all character data
        data_fetched = 0
        for mal_id in mal_ids:
            character_data = self.fecth_character_data(mal_id)
            data_fetched += 1
            print(f"Fetched character data {data_fetched} out of {len(self.anime_data)}")
            if character_data is not None:
                character_df = pd.DataFrame(character_data)
                character_df['mal_id'] = mal_id
                all_character_data = pd.concat([all_character_data, character_df], ignore_index=True)
        self.character_data = all_character_data   
        return all_character_data

    def fetch_reviews_data(self, mal_id):
        """Fetch reviews data for a specific anime using the Jikan API."""
        try:
            reviews_data = self.jikan.anime(mal_id, extension='reviews')
            time.sleep(0.8)  # Add a delay of 1 second before fetching the next reviews data
            return reviews_data['data']
        except APIException as e:
            print(f"Error fetching reviews data for {mal_id}: {e}")
            return None
        
    def fetch_all_reviews_data(self, mal_ids):
        """Fetch reviews data for multiple anime using the Jikan API."""
        all_reviews_data = pd.DataFrame()
        data_fetched = 0
        for mal_id in mal_ids:
            reviews_data = self.fetch_reviews_data(mal_id)
            data_fetched += 1
            print(f"Fetched reviews data {data_fetched} out of {len(self.anime_data)}")
            if reviews_data is not None:
                reviews_df = pd.DataFrame(reviews_data)
                reviews_df['anime_id'] = mal_id
                all_reviews_data = pd.concat([all_reviews_data, reviews_df], ignore_index=True)
        self.reviews_data = all_reviews_data
        return all_reviews_data
    
    def extract_reviews_info(self):
        """Extract relevant reviews information into a DataFrame, with 'tags' as individual values."""
        
        reviewsInformation = {
            'anime_id': self.reviews_data['anime_id'],
            'review_id': self.reviews_data['mal_id'],
            'score': self.reviews_data['score'],
            'is_spoiler': self.reviews_data['is_spoiler'],
            'is_preliminary': self.reviews_data['is_preliminary'],
            'episodes_watched': self.reviews_data['episodes_watched'],
            'tags': self.reviews_data['tags'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None),  # Extract first value
            'review_text': self.reviews_data['review']
        }

        reviewsInformation = pd.DataFrame(reviewsInformation)
        return reviewsInformation

        
    def extract_character_info(self):
        """Extract relevant character information into a DataFrame."""
        characterInformation = {
            'anime_id': self.character_data['mal_id'],
            'character_id': self.character_data['character'].apply(lambda x: x['mal_id'] if isinstance(x, dict) and 'mal_id' in x else None),
            'name': self.character_data['character'].apply(lambda x: x['name'] if isinstance(x, dict) and 'name' in x else None),
            'role': self.character_data['role'], 
            'favorites': self.character_data['favorites']
        }
        characterInformation = pd.DataFrame(characterInformation)
        return characterInformation
    
    def extract_VA_info(self):
        """Extract relevant VA information into a DataFrame."""

        VAInformation = {
            'character_id': self.character_data['character'].apply(lambda x: x['mal_id'] if isinstance(x, dict) and 'mal_id' in x else None),
            'voice_actor_info': self.character_data['voice_actors'].apply(
                lambda x: [(actor['person']['mal_id'], actor['person']['name'], actor['language'])
                        for actor in x if isinstance(actor, dict) and 'person' in actor and 'mal_id' in actor['person'] and 'language' in actor]
                if isinstance(x, list) else []),
            
        }

        # Create the DataFrame from the extracted information
        VA_df = pd.DataFrame(VAInformation)

        # Explode the voice actor info into separate columns
        VA_df = VA_df.explode('voice_actor_info')

        # Split the tuples into separate columns
        VA_df[['voice_actor_id', 'voice_actor_name', 'voice_actor_language']] = pd.DataFrame(
            VA_df['voice_actor_info'].tolist(), index=VA_df.index)

        # Drop the original 'voice_actor_info' column
        VA_df.drop(columns=['voice_actor_info'], inplace=True)

        # Convert voice_actor_id to integer to remove the .0 issue
        VA_df['voice_actor_id'] = VA_df['voice_actor_id'].astype('Int64')

        # Reset index if needed
        VA_df.reset_index(drop=True, inplace=True)

        # Select only the relevant columns
        VA_df = VA_df[['character_id', 'voice_actor_id', 'voice_actor_name', 'voice_actor_language']]

        return VA_df

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
            
    
    # def extract_VA_info(self):
    #     """Extract relevant VA information into a DataFrame without using explode."""
        
    #     character_ids = []
    #     voice_actor_ids = []
    #     voice_actor_names = []
    #     voice_actor_languages = []

    #     # Loop over each row of self.character_data
    #     for _, row in self.character_data.iterrows():
    #         character_id = row['character']['mal_id'] if isinstance(row['character'], dict) and 'mal_id' in row['character'] else None
    #         voice_actors = row['voice_actors'] if isinstance(row['voice_actors'], list) else []

    #         # Loop through the voice actors and extract the information
    #         for actor in voice_actors:
    #             if isinstance(actor, dict) and 'person' in actor and 'mal_id' in actor['person'] and 'language' in actor:
    #                 character_ids.append(character_id)
    #                 voice_actor_ids.append(actor['person']['mal_id'])
    #                 voice_actor_names.append(actor['person']['name'])
    #                 voice_actor_languages.append(actor['language'])

    #     # Create a DataFrame from the lists
    #     VA_df = pd.DataFrame({
    #         'character_id': character_ids,
    #         'voice_actor_id': voice_actor_ids,
    #         'voice_actor_name': voice_actor_names,
    #         'voice_actor_language': voice_actor_languages
    #     })

    #     # Convert voice_actor_id to integer to remove the .0 issue
    #     VA_df['voice_actor_id'] = VA_df['voice_actor_id'].astype('Int64')

    #     return VA_df