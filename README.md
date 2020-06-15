# Movies-ETL - Challenge 8 
This challenge consisted of building a function which automated the processes discussed in this module. Python 3.7 with the Anaconda (Pandas) package was used to perform the analysis. A python file containing the function can be found in this repository. 

The processes were aimed at importing JSON and CSV files, cleaning the data by eliminating null values, and eliminating redundicies within the data, creating data tables through merging cleaned data sets, and then exporting these data tables into PostgreSQL.

A final step was considered which consisted of utilizing try-except blocks and other predictive statements to account for unforeseen problems that may arise in the event that the data changes, whether that being structure wise, or formatting wise. 

The following assumptions were made and were accounted for in the function discussed above: 

1. Wiki_data (wiki data input to function) isn't enterred as JSON format, but instead enterred as pd.DataFrame. This will cause the following functions to incorrectly solve resulting in a failed function. 
  - A try and except block is used to convert the dataframe back to a JSON format using the DataFrame.to_json function 

2. If a user inputs the month portion of the date as a three letter abbreviation instead of the full complete spelling, the majority of the data will be left out. 
  - The statements including the 3 letter abbreviations are then included to account for this assumption. 

3. Money formats such as M$##, etc aren't account for with the preivous forms. This would result in data being missed in our analysis. 
  - In order to account for this, additional forms must be made and the related changes made to the code in the other areas.

4. The kaggle datatype transfer section of the function is susceptible to obtaining an error if there are any NaN values. This would result in a halt of the function until those values are fixed. 
  - To fix this, a try and except block is created which will convert the NaN values to zero if there is any error when initially computing the datatype conversions. 
