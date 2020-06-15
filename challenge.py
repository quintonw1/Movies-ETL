# Automated ETL pipeline function for Challenge 8 
# Import Dependicies
import pandas as pd
import numpy as np
import json 
import re
file_dir = 'C:/Users/quint/OneDrive/Documents/University of Toronto/Class Folder/Movies-ETL/'

# Importing wikikpedia file into wiki_movies_raw
with open(f'{file_dir}wikipedia.movies.json', mode = 'r') as file: 
    wiki_movies_raw = json.load(file)

# Using pandas pd.read_csv, open the kaggle and ratings data 
kaggle_metadata = pd.read_csv(f'{file_dir}Data/movies_metadata.csv', low_memory = False)
ratings = pd.read_csv(f'{file_dir}Data/ratings.csv')

# Create a function that takes in three arguments (Wiki data, Kaggle Data, and Ratings data)
# Perform filtering analysis 
def filtering_funct(wiki_data, kaggle_metadata, rating_data):
    # Filter the columns to only include those that contain "Director", "Directed by", imbd_links,
    # and excluding TV series (No. of Episodes)

    #Initiate cleaning function 
    def clean_movie(movie):
        # Create a non-destructive copy 
        movie = dict(movie)
        # Adding alternate titles to dataframe
        # by using the alt_keys list and cross referencing them against the 
        # columns   
        alt_titles = {}
        alternate_keys = ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']
        for key in alternate_keys:
            if key in movie: 
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0: 
            # Adding newly acquired alternate title to df
            movie['alt_titles'] = alt_titles
        
        # Changing column names to more readable and more accurate words by
        # merging them with a new function 
        def change_column_name(old_name, new_name):
            if old_name in movie: 
                movie[new_name] = movie.pop(old_name)
        
        # Call change_column_name function to change the column's names
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')  

        # Return cleaned movie
        return movie

    ## Assumption 1: wiki_data isn't enterred as JSON format, but instead enterred as pd.DataFrame 

    #Obtain list of movies which meet above criteria:
    try: 
        wiki_movies = [movie for movie in wiki_data if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]
        # Call clean_movie function for each movie in wiki_df
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
    except: 
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
        wiki_data = wiki_data.to_json(orient = 'split') 
        wiki_movies = [movie for movie in wiki_data if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]
        # Call clean_movie function for each movie in wiki_df
        clean_movies = [clean_movie(movie) for movie in wiki_movies]


    # Create a new dataframe with the cleaned movies dataframe
    wiki_df = pd.DataFrame(clean_movies)
    # Extract imdb_ids from imdb link and add to df under new column 
    # 'imdb_id'
    wiki_df['imdb_id'] = wiki_df['imdb_link'].str.extract(r'(tt\d{7})')

    # Drop Duplicate rows 
    wiki_df.drop_duplicates(subset = 'imdb_id', inplace = True)

    # Eliminate columns with more than 90% NaN or Null values 
    wiki_columns_to_keep = [column for column in wiki_df.columns if wiki_df[column].isnull().sum() < len(wiki_df) * 0.9]
    wiki_df = wiki_df[wiki_columns_to_keep]
    # Create parse dollars function
    def parse_dollars(s): 
        # If s is not a string, return NaN 
        if type(s) != str:
            return np.nan
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and "million"
            s = re.sub(r'\$|\s|[a-zA-Z]', '', s)

            # convert to float and multiply by a million 
            value = float(s) * 10**6

            # Return value 
            return value 
        if re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags = re.IGNORECASE):
            s = re.sub(r'\$|\s|[a-zA-Z]', '', s)
        
            # convert to float and multiply by a million 
            value = float(s) * 10**9
        
            # return value
            return value  
        
        elif re.match(r'\$\s\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)', s, flags = re.IGNORECASE):
            # remove dollar sign and commas 
            s = re.sub(r'\$|,', '', s)
        
            # convert to float 
            value = float(s)
        
            # return value 
            return value 
    
        # otherwise, return NaN 
        else: 
            return np.nan

    ## Start modifying columns 
    
    ## Box_office
    box_office = wiki_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Create regex forms for financial searches 
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)'
    
    # Remove ranged values
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex= True)

    # Utilize the parse_dollars function to revise the box_office values 
    # Drop 'Box office' column
    wiki_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_df.drop('Box office', axis=1, inplace=True)
    
    ## Budget Data Cleaning 

    budget = wiki_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex = True)
    budget = budget.str.replace(r'\[\d+\]\s*','')

    # modify wiki_df for new values and drop old column
    wiki_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_df.drop('Budget', axis = 1, inplace = True)

    ## Release Date Cleaning 
    release_date = wiki_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Creating data forms for release date
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|December)\s\d{4}'
    date_form_four = r'\d{4}'  
    # Assumption 2: user inputs month with three letter abbreviation instead of complete spelling 
    # Must add additional search results 
    date_form_five = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Dec)\s[123]\d,\s\d{4}'
    date_form_six = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Dec)\s\d{4}'


    # Modify wiki_df for new values and drop old column 
    wiki_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four}|{date_form_five}|{date_form_six})',flags=re.IGNORECASE)[0], infer_datetime_format = True)
    wiki_df.drop('Release date', axis =1, inplace = True)

    ## Running Time Cleaning 
    running_time = wiki_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Creating data forms for running time 
    r = r'(\d+)\sho?u?r?s?\s*(\d*)|(\d+)\s*m'

    # Extracting running time data 
    running_time_extract = running_time.str.extract(r)
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors = 'coerce')).fillna(0)

    # Add running_time data to new column and delte the old 
    wiki_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] ==0 else row[2], axis = 1)
    wiki_df.drop('Running time', axis = 1 , inplace = True)

    ### Start analysis of Kaggle Metadata

    # filtering for non-adult movies
    # remove adult column 
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis = 'columns')

    # Converting data types 
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'   
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int) 
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors = 'raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors = 'raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    # Merge the kaggle_metadata and wiki_df tables 
    movies_df = pd.merge(wiki_df, kaggle_metadata, on = 'imdb_id', suffixes =['_wiki', '_kaggle'])

    ### Begin cleanup of merged dataframe 

    # Drop outlier row from prior release_date_wiki analysis 
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # Drop columns from wiki_data 
    movies_df.drop(columns = ['title_wiki', 'release_date_wiki', 'Language', 'Production company(s)'], inplace= True)

    # Create function to fill_missing_kaggle_data
    def fill_missing_kaggle_data (df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis = 1)
            df.drop(columns=wiki_column, inplace= True)

    # Run above function for the three columns 
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # Drop non video rows
    movies_df['video'].value_counts(dropna=False)

    # Re-ordering columns 
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]
    # Rename columns 
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                    }, axis='columns', inplace=True)

    # Ratings modifications 
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit = 's')

    # Not sure if this is a requirement for the CHallenge....rating_counts or movies_with_ratings_df aren't exported to SQL so maybe not???
    rating_counts = ratings.groupby(['movieId', 'rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId', columns= 'rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on = 'kaggle_id', right_index = True, how = 'left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)


    from sqlalchemy import create_engine
    
    # import password from config file 
    from config import db_password

    # create connection string to sql postgre
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/Movies"
    engine = create_engine(db_string)

    # Import table into SQL 
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html used for if_exists statement within to_sql

    movies_df.to_sql(name='movies', con=engine, if_exists = 'replace')

    # Import time 
    import time 

    rows_imported = 0
    # export ratings csv to sql
    # Get start time from time.time()
    start_time = time.time()

    # Clear Existing Table in SQL
    refresh_table_columns = ratings.columns.tolist()
    refresh_table = pd.DataFrame(columns = refresh_table_columns)
    refresh_table.to_sql(name='ratings', con=engine, if_exists= 'replace')

    for data in pd.read_csv(f'{file_dir}Data/ratings.csv', chunksize=1000000):
        # Print out the range of rows that are being imported 
    
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    
        data.to_sql(name='ratings', con=engine, if_exists= 'append')
    
        # increment the number of rows imported by the chunksze 
    
        rows_imported += len(data)
    
        # print that the rows have finished importing and add elapsed solve time 
        print(f'Done. {time.time() - start_time} total seconds elapsed')

filtering_funct(wiki_movies_raw, kaggle_metadata, ratings)