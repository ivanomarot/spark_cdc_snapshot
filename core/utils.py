from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import List, Iterable, Callable

import boto3


def parse_date(string: str):
    """
    :param string: string to be parsed as datetime
    :returns datetime
    """
    return datetime.strptime(string, '%Y-%m-%d')


def run_multi_threaded_map(mapped_function: Callable, args_mapped_function: Iterable, thread_number: int):
    pool = ThreadPool(thread_number)
    pool.map(mapped_function, args_mapped_function)
    pool.close()
    pool.join()


def list_path_files(s3_path: str) -> List[str]:
    """
    :param s3_path: s3 prefix
    :returns list: list of s3 path corresponding to the objects under the s3 prefix
    """
    bucket_name, prefix = s3_path.split('/', 3)[-2:]
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return [f's3://{bucket_name}/{object_summary.key}' for object_summary in bucket.objects.filter(Prefix=prefix)]
