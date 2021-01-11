import io
import os
import sys
from datetime import datetime
from uuid import uuid4

import boto3
import pandas as pd
from awsglue.utils import getResolvedOptions


def main():
    params = getResolvedOptions(sys.argv, ['target-bucket-name'])

    date = datetime.now()
    path = os.path.join(
        'products', 'year=' + date.strftime('%Y'), 'month=' + date.strftime('%m'), 'day=' + date.strftime('%d'),
                    'hour=' + date.strftime('%H'), 'minute=' + date.strftime('%M'), str(uuid4()) + '.json',
    )

    items = [generate_item(date, i) for i in range(5)]
    df = pd.DataFrame(items)

    upload_file(params['target_bucket_name'], path, df)


def generate_item(date: datetime, i: int):
    return {
        'id': f'{date.strftime("%Y%m%d%H%M")}-{i}',
        'name': f'Product {i} from {date.strftime("%Y-%m-%d %H:%M")}'
    }


def upload_file(bucket_name: str, path: str, df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_json(buffer, orient='records', lines=True)

    s3 = boto3.client('s3', api_version='2006-03-01')
    s3.put_object(
        Bucket=bucket_name,
        Key=path,
        Body=buffer.getvalue(),
    )

    buffer.close()


main()
