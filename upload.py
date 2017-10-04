import boto3
import sys
import time

# We assume the presence of an index column.
# Set this global variable to false if the input data does not have one.
index_column = True

# Table to upload
table_name = 'lego_objective_recommender_features'

# Expiration time in seconds
expiration_interval = 300

# Source file to upload
file_path = "data/tuned_input_features.tsv"

def upload_row(dynamo_table, client_attributes, profile_env="test"):
    '''
    Upload a single row to dynamo_table.  Row is specified as 
    the client_attributes dictionary.
    '''
    
    session = boto3.Session(profile_name=profile_env)
    dynamodb = session.resource('dynamodb', region_name='us-east-1')
    
    table = dynamodb.Table(dynamo_table)
    # Create a table row by first copying client_attributes, then
    # adding a timestamp and an expiration.
    row_data = dict(client_attributes)
    row_data.update({
        'date_loaded': int(time.time()),
        'exp_date': int(time.time() + expiration_interval)
    })    
    table.put_item(
        Item = row_data
    )


with open(file_path, "r") as f:
    # We assume the presence of a header row that gives feature names.
    # Feature names are likely to be the result of a dimensionality reduction
    # operation.  Features may have names like, "claims1", "claims2" etc.
    header = f.readline()
    keys = header.lower().strip().split('\t')

    for row in f:
        client_attributes = {}
        values = row.strip().split('\t')
        
        # If index_column is true (1), we slice the values to remove the first column.    
        # If false (0) the slicing doesn't do anything.
        values = values[int(index_column):]

        # Iterate through key, value tuples, constructing a dictionary of client attributes
        # to upload to the table.
        for k,v in zip(keys, values):
            client_attributes [k] = v
        upload_row(table_name, client_attributes)
