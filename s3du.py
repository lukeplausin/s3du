#!/usr/bin/env python

import boto3
from multiprocessing import Pool, Semaphore

import datetime
import dateutil

tzutc = dateutil.tz.tz.tzutc()

def blank_counter(Key):
    if not Key:
        Key = "."
    return {
        "N": 0,
        "Size": 0,
        "Oldest": datetime.datetime.now(tz=tzutc),
        "Newest": datetime.datetime(year=1990, month=1, day=1).astimezone(tzutc),
        "Key": Key
    }

def single_counter(s3_object):
    return {
        "N": 1,
        s3_object['StorageClass']: s3_object['Size'],
        "Size": s3_object['Size'],
        "Oldest": s3_object['LastModified'],
        "Newest": s3_object['LastModified'],
        "Key": s3_object['Key']
    }

def count_object(counter, s3_object):
    counter["N"] = counter["N"] + 1
    counter["Size"] = counter["Size"] + s3_object['Size']
    counter["Oldest"] = min(counter["Oldest"], s3_object['LastModified'])
    counter["Newest"] = max(counter["Newest"], s3_object['LastModified'])
    counter[s3_object['StorageClass']] = counter.get(s3_object['StorageClass'], 0) + s3_object['Size']

def count_summary(counter, s3_object_summary):
    for key, value in s3_object_summary.items():
        if key == "Key":
            pass
        elif key == "Oldest":
            counter["Oldest"] = min(counter["Oldest"], s3_object_summary['Oldest'])
        elif key == "Newest":
            counter["Newest"] = max(counter["Newest"], s3_object_summary['Newest'])
        else:
            counter[key] = counter.get(key, 0) + value


def flatten_file_stats(page, client):
    stats = blank_counter(page['Prefix'])

    for s3_object in page.get('Contents', []):
        count_object(stats, s3_object)

    if page['IsTruncated']:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=page['Name'],
            Delimiter=page['Delimiter'],
            Prefix=page['Prefix'],
            PaginationConfig={
                'StartingToken': page['NextContinuationToken']
            }
        )
        for page in page_iterator:
            for s3_object in page.get('Contents', []):
                count_object(stats, s3_object)
    return stats


def collate_file_stats(page, client):
    for s3_object in page.get('Contents', []):
        yield single_counter(s3_object)


def s3_disk_usage_recursive(client, 
        Bucket, Depth=None, Delimiter="/", Prefix="", flatten_large_results=True):
    # Calculate disk usage within S3 and report back to parent
    node_sizes = blank_counter(Prefix)
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Delimiter=Delimiter, Prefix=Prefix)

    for page in page_iterator:
        # Do something with the contents of this prefix
        for prefix in page.get('CommonPrefixes', []):
            gen_subkey_items = s3_disk_usage_recursive(
                client=client,
                Bucket=Bucket,
                Delimiter=Delimiter,
                Depth=(Depth - 1),
                Prefix=prefix['Prefix']
            )
            for entry in gen_subkey_items:
                # Add totals to this node
                count_summary(node_sizes, entry)
                if not (Depth and (Depth <= 0)):
                    # Produce details of the subkeys
                    yield entry
            
        # Deal with the files
        if page['IsTruncated']:
            # Too many files to display nicely
            if (Depth and (Depth <= 0)) or flatten_large_results:
                # TODO - Delegate work to a new thread to count and yield the thread
                count_summary(node_sizes, flatten_file_stats(page, client))
                break
            else:
                for entry in collate_file_stats(page, client):
                    count_summary(node_sizes, entry)
                    # Produce details of the subkeys
                    yield entry

        else:
            # Can count these easily
            if Depth and (Depth <= 0):
                count_summary(node_sizes, flatten_file_stats(page, client))
            else:
                for entry in collate_file_stats(page, client):
                    count_summary(node_sizes, entry)
                    # Produce details of the subkeys
                    yield entry
    yield node_sizes


def s3_disk_usage(Bucket, Depth=None, Delimiter="/", Prefix="", client=boto3.client('s3'), max_processes=12):
    # Calculate disk usage within S3 and report back
    return s3_disk_usage_recursive(
        Bucket=Bucket,
        Depth=(Depth-1),
        Delimiter=Delimiter,
        Prefix=Prefix,
        client=client
        #semaphore=Semaphore(max_processes)
    )

if __name__ == "__main__":
    try:
        import argparse 
    except ImportError:
        print("ERROR: You are running Python < 2.7. Please use pip to install argparse:   pip install argparse")

    parser = argparse.ArgumentParser(add_help=True, description="Display S3 usage by storage tier")
    parser.add_argument("--depth", "-d", type=int, help="Maximum depth (0 by default)", default=-1)
    parser.add_argument("--bucket", type=str, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, help="S3 bucket prefix", default="")
    parser.add_argument("--delimiter", type=str, help="S3 bucket delimiter", default="/")
    parser.add_argument("--truncate", type=bool, help="Summarise keys with over 1000 results?", default=True)

    args = parser.parse_args()
    if args.depth < 0:
        depth = None
    else:
        depth = args.depth

    client = boto3.client('s3')
    for statistic in s3_disk_usage_recursive(client, 
            args.bucket, Depth=(depth-1), Delimiter=args.delimiter, Prefix=args.prefix, flatten_large_results=True):
        # print("Key: {Key}, Size: {Size}, N: {N}, Oldest: {Oldest}, Newest: {Newest}".format(**statistic))
        print("b: {Size:>16} N: {N:>13} {Key:>60}   O: {Oldest:%Y-%m-%d} N: {Newest:%Y-%m-%d}".format(**statistic))
