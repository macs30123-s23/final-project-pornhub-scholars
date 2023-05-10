import pandas as pd
import mysql.connector

def lambda_handler(event, context):
    # Connect to MySQL database
    cnx = mysql.connector.connect(
        host='your-hostname',
        user='your-username',
        password='your-password',
        database='your-database'
    )
    
    # Load data into Pandas dataframe
    df = pd.read_sql('SELECT * FROM your-table', con=cnx)
    
    # Clean text data
    df['text_column'] = df['text_column'].apply(lambda x: x.lower()) # convert text to lowercase
    df['text_column'] = df['text_column'].apply(lambda x: re.sub(r'[^\w\s]','',x)) # remove punctuation
    df['text_column'] = df['text_column'].apply(lambda x: re.sub(r'\d+', '', x)) # remove numbers
    
    # Write cleaned data back to MySQL database
    df.to_sql(name='your-table', con=cnx, if_exists='replace', index=False)
    
    # Close MySQL connection
    cnx.close()
    
    return 'Data cleaned successfully'