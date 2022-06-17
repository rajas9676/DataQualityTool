# DataQualityTool

Following python script would help address the data quality issues in the dataset 'Trending youtube shows information in India' based on the following criteria.

1. Data de-duplication - Removal of records with similar values in all the columns
2. Missing data - Identify  the missing information in all the columns using heat map and remove corresponding records
3. Remove all the records with an active error indicator(comments disabled, rating disabled, video deleted)
4. Drop all irrelevant columns including the columns with error indicator values
5. Remove all non-ASCII characters from columns with textual information. 
6. Handle dirty data with lookup values and retain only english and printable characters from description fields.
7. Create a new column populating title of the video category corresoponding to the id and title information maintained in json file.
8. Implement logging mechanism creating log file with execution results.
9. Export the resulting clean dataframe into parquet/csv format for further processing.

Input : All the input files to be placed in this folder
Output : Resulting files to be exported in this folder

Windows users execute the following command to make sure that all the prerequisites are met

    pip install -r requirements.txt
  
Execute the following command post navigating to the directory  with main.py file

    python main.py



