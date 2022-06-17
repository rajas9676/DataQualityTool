import os
import pandas as pd
import numpy as np
import seaborn as sns
import logging
import json
import matplotlib.pyplot as plt

# create and configure logging
logging.basicConfig(filename="testrun.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logging.getLogger('matplotlib.font_manager').disabled = True
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def handle_duplicates(data):
    # check if the file contains duplicate rows
    dup_rows = data[data.duplicated()]
    logger.info(f"A total of {dup_rows.shape[0]} duplicate rows present in the dataset")
    # Remove duplicate records keeping only first record of duplicate set
    try:
        dedup_data = data.drop_duplicates(keep='first')
        print(dedup_data.shape)
        logger.info(f"Number of records after removing duplicates: {dedup_data.shape[0]}")
        return dedup_data
    except Exception as e:
        logging.exception(e)


def handle_missing_data(data):
    # Visualize the missing data of each column using heat map
    cols = data.columns
    colours = ['#000099', '#ffff00']  # specify the colours - yellow is missing. blue is not missing.
    sns.heatmap(data[cols].isnull(), cmap=sns.color_palette(colours))
    plt.show()
    # check of the percentage of missing data in each column
    for col in data.columns:
        null_pct = np.mean(data[col].isnull())
        if round(null_pct * 100) > 0:
            logger.info('{}% of data in {} column contains null values'.format(round(null_pct * 100), col))
    # drop the rows with missing values in any column
    try:
        data_fix_null = data.dropna()
        logger.info(f"Number of records after removing Null values: {data_fix_null.shape[0]}")
        return data_fix_null
    except Exception as e:
        logger.exception(e)


def handle_ratings_comments_disabled(data):
    # Get the records without any video errors
    try:
        df_no_error = data[data['video_error_or_removed'] == False]
        # Get all the records without either comments or ratings disabled
        df_no_error = df_no_error[(df_no_error['comments_disabled'] == False) &
                                  (df_no_error['ratings_disabled'] == False)]
        logger.info(f"Number of records without comments/ratings disabled: {df_no_error.shape[0]}")
        return df_no_error
    except Exception as e:
        logger.exception(e)


def remove_irrelevant_cols(data):
    # Remove irrelevant columns from clean data
    remove_cols = ['thumbnail_link', 'comments_disabled',
                   'ratings_disabled', 'video_error_or_removed']
    try:
        result = data.drop(columns=remove_cols, axis=1)
        logger.info('Successfully removed irrelevant columns')
        return result
    except Exception as e:
        logger.exception(e)


def handle_dirty_data(data):
    # remove pipe character from tags column
    data.loc[data['tags'] == '[None]', 'tags'] = ' '
    # split tags with '|' and convert list to one string
    data['tags'] = [' '.join(tag) for tag in data['tags'].str.split('|')]
    lookup = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n'
    dirty_cols = ['title', 'channel_title', 'description', 'tags']
    # Delete all the Non-ASCII characters based on the lookup from dirty columns
    for col in dirty_cols:
        data[col] = data[col].apply(lambda x: ''.join(filter(lambda xi: xi in lookup, x)))
    symbols = [c for c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"]
    data = data[data['title'].str.contains('|'.join(symbols))]
    data = data[data['channel_title'].str.contains('|'.join(symbols))]
    return data


def get_categories():
    # get the category details from json file
    result = {}
    try:
        with open("input/IN_category_id.json", "r") as read_file:
            data = json.load(read_file)
            for values in data['items']:
                result[int(values['id'])] = values['snippet']['title']
        return result
    except FileExistsError:
        logger.error('Missing file for category information')
        return False


def assign_category_details(data):
    lookup = get_categories()
    if lookup:
        data['category'] = data['category_id'].map(lookup)
    return data


if __name__ == "__main__":
    # check whether the file is present for further processing
    input_file_path = 'input/IN_videos.csv'
    if os.path.exists(input_file_path):
        logging.info('Valid file present for processing')
        input_data = pd.read_csv(input_file_path)
        print(input_data.shape)
        print(input_data.columns)
        # Handle duplicate rows in input file
        data_dedup = handle_duplicates(input_data)

        # Handle missing values from deduplicated file
        data_without_null = handle_missing_data(data_dedup)

        # Handle records with comments or ratings disabled
        data_part_clean = handle_ratings_comments_disabled(data_without_null)

        # Remove irrelevant columns from the dataset
        data_drop_cols = remove_irrelevant_cols(data_part_clean)

        # Handle dirty data
        data_final_clean = handle_dirty_data(data_drop_cols)

        # Assign category details based  on lookup from json file
        result = assign_category_details(data_final_clean)

        # store the results in parquet file
        try:
            result.to_parquet('output/clean_file.parquet')
            logging.info('Successfully created parquet file with clean data')
        except Exception as e:
            logging.exception(e)

        result.to_csv('tmpresult.csv', index=False, header=True)
    else:
        logging.error('Please place an input for processing')
