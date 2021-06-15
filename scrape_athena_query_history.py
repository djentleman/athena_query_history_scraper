import sys
import boto3
import time
import pandas as pd

# single argument is the number of records to scrape per workgroup
# defaults to 500
# usage: scrape_athena_query_history.py 5000

# reverse engineered from https://github.com/awslabs/aws-data-wrangler/blob/main/awswrangler/athena/_read.py#L116

ac = boto3.client('athena')
paginator = ac.get_paginator("list_query_executions")
# change this to scrape more
try:
    to_scrape = int(sys.argv[1])
except:
    print('Invalid Argument, defaulting to 500 queries per workgroup')
    to_scrape = 500
# max page size is 50
page_size = 50

cols = [
    'execution_id',
    'output_location',
    'database',
    'status',
    'submission_time',
    'completion_time',
    'engine_execution_time',
    'data_scanned',
    'total_execution_time',
    'query_queue_time',
    'query_planning_time',
    'workgroup', 
    'engine_version'
]

required_data = {
    'execution_id': ['QueryExecutionId'],
    'output_location': ['ResultConfiguration', 'OutputLocation'],
    'database': ['QueryExecutionContext', 'Database'],
    'status': ['Status', 'State'],
    'submission_time': ['Status', 'SubmissionDateTime'],
    'completion_time': ['Status', 'CompletionDateTime'],
    'engine_execution_time': ['Statistics', 'EngineExecutionTimeInMillis'],
    'data_scanned': ['Statistics', 'DataScannedInBytes'],
    'total_execution_time': ['Statistics', 'TotalExecutionTimeInMillis'],
    'query_queue_time': ['Statistics', 'QueryQueueTimeInMillis'],
    'query_planning_time': ['Statistics', 'QueryPlanningTimeInMillis'],
    'workgroup': ['WorkGroup'],
    'engine_version': ['EngineVersion', 'EffectiveEngineVersion']
}

def get_value(metadata, path):
    for item in path:
        metadata = metadata.get(item)
        if metadata is None:
            return None
    return metadata

def to_df_row(query_metadata):
    return [get_value(query_metadata, required_data[c]) for c in cols]


df_rows = []
i = 0
for workgroup in [w['Name'] for w in ac.list_work_groups()['WorkGroups']]:
    print(f'running {workgroup}')
    args = {
        "WorkGroup": workgroup, "PaginationConfig": {"MaxItems": to_scrape, "PageSize": page_size}
    }
    for page in paginator.paginate(**args):
        query_ids = page['QueryExecutionIds']
        for query_id in query_ids:
            i += 1
            query_metadata = ac.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
            df_row = to_df_row(query_metadata)
            df_rows.append(df_row)
        print(f'processed {i} queries')
        time.sleep(1) # dont get throttled
df = pd.DataFrame(df_rows, columns=cols)
df.to_csv('athena_scraped_results.csv', index=False)
