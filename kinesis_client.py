import boto3
import json

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Stream name
STREAM_NAME = 'Word2Stocks'

def put_record(data, partition_key):
    """
    Put a single record into the Kinesis stream.
    """
    response = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=partition_key
    )
    print("PutRecord Response:", response)

def get_records(shard_id, iterator_type='LATEST'):
    """
    Get records from the Kinesis stream.
    """
    # Get shard iterator
    shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType=iterator_type
    )['ShardIterator']

    # Get records
    records_response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=10
    )
    for record in records_response['Records']:
        print("Record:", record['Data'])

if __name__ == "__main__":
    # Example: Put a record
    put_record({'message': 'Hello, Kinesis!'}, partition_key='exampleKey')

    # Example: Get records (replace 'shardId-000000000000' with your actual ShardId)
    # get_records('shardId-000000000000')