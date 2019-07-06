#!/usr/bin/env python

import boto3
from multiprocessing import Pool, Semaphore

import datetime
import dateutil
import os
import json


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
            if page.get('Contents', None):
                for s3_object in page.get('Contents'):
                    count_object(stats, s3_object)
    return stats


def file_prefix_stats(client, Prefix, Bucket):
    stats = blank_counter(Prefix)
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=Bucket,
        Prefix=Prefix
    )
    for page in page_iterator:
        for s3_object in page.get('Contents', []):
            count_object(stats, s3_object)
    return stats


def collate_file_stats(page, client):
    for s3_object in page.get('Contents', []):
        yield single_counter(s3_object)


def s3_disk_usage( 
            Bucket, Depth=float('Inf'), Delimiter="/", Prefix="", flatten_large_results=True,
            client=boto3.client('s3')
        ):
    # Calculate disk usage within S3 and report back to parent
    node_sizes = blank_counter(Prefix)
    try:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=Bucket, Delimiter=Delimiter, Prefix=Prefix)

        for page in page_iterator:
            # Do something with the contents of this prefix
            for prefix in page.get('CommonPrefixes', []):
                if Depth <= 1:
                    # Don't need to go into detail.
                    # TODO: Fork here
                    subkey_stats = file_prefix_stats(
                        client=client,
                        Bucket=Bucket,
                        Prefix=prefix['Prefix']
                    )
                    count_summary(node_sizes, subkey_stats)
                    if Depth > 0:
                        yield subkey_stats
                else:
                    gen_subkey_items = s3_disk_usage(
                        client=client,
                        Bucket=Bucket,
                        Delimiter=Delimiter,
                        Depth=(Depth - 1),
                        Prefix=prefix['Prefix']
                    )
                    for entry in gen_subkey_items:
                        # Add totals to this node (if prefix matches keyy)
                        if (len(entry['Key']) == len(prefix['Prefix'])):
                            count_summary(node_sizes, entry)
                        # Produce details of the subkeys
                        yield entry
                
            # Deal with the files
            if page.get('Contents', None):
                if page['IsTruncated']:
                    # Too many files to display nicely
                    if Depth <= 0 or flatten_large_results:
                        # TODO - Delegate work to a new thread to count and yield the thread
                        count_summary(node_sizes, flatten_file_stats(page, client))
                        break
                    else:
                        for entry in collate_file_stats(page, client):
                            count_summary(node_sizes, entry)
                            # Produce details of the subkeys
                            if len(entry['Key']) != len(Prefix):
                                yield entry

                else:
                    # Can count these easily on same process
                    if Depth <= 0:
                        count_summary(node_sizes, flatten_file_stats(page, client))
                    else:
                        for entry in collate_file_stats(page, client):
                            count_summary(node_sizes, entry)
                            # Produce details of the subkeys
                            if len(entry['Key']) != len(Prefix):
                                yield entry
    except Exception as e:
        print("Exception counting objects in s3://{}/{}".format(Bucket, Prefix))
        print("Exception: {}".format(e))
    yield node_sizes


def human_bytes(size, base=2):
    # 2**10 = 1024
    if base == 2:
        power = 2**10
        power_labels = {0 : '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}
    elif base == 10:
        power = 10**3
        power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    else:
        raise ValueError("Unknown base {}".format(base))

    n = 0
    while size > power:
        size /= power
        n += 1
    return "{:.1f} {:>2}".format(size, power_labels[n])


def main():
    try:
        import argparse 
    except ImportError:
        print("ERROR: You are running Python < 2.7. Please use pip to install argparse:   pip install argparse")

    parser = argparse.ArgumentParser(add_help=True, description="Display S3 usage by storage tier")
    parser.add_argument("--depth", "-d", type=int, help="Maximum depth (0 by default)", default=-3)
    parser.add_argument("--human", help="Human readable sizes", default=False, action='store_true')
    parser.add_argument("--bucket", type=str, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, help="S3 bucket prefix", default="")
    parser.add_argument("--delimiter", type=str, help="S3 bucket delimiter", default="/")
    parser.add_argument("--truncate", type=bool, help="Summarise keys with over 1000 results?", default=True)
    parser.add_argument("--file", "-f", type=str, help="File name to output data to", default="")

    args = parser.parse_args()
    if args.depth <= -3:
        depth = float('Inf')
    else:
        depth = args.depth

    client = boto3.client('s3')
    if args.file:
        output_file = args.file
    else:
        output_file = os.devnull
    append_object = False
    
    with open(output_file, 'w') as f:
        f.write("[\n")
        for statistic in s3_disk_usage( 
                Bucket=args.bucket,
                Depth=depth,
                Delimiter=args.delimiter,
                Prefix=args.prefix,
                flatten_large_results=True,
                client=client
            ):
            # Write to stdout
            if args.human:
                size = human_bytes(statistic['Size'])
                number = human_bytes(statistic['N'], base=10)
            else:
                size = statistic['Size']
                number = statistic['N']
            print("b: {PrintSize:>16}B N: {PrintNumber:>13} {Key:>60}   O: {Oldest:%Y-%m-%d} N: {Newest:%Y-%m-%d}".format(
                PrintSize=size, PrintNumber=number, **statistic))
            # Write to output file
            if append_object:
                f.write(",\n")
            else:
                append_object = True
            json.dump(statistic, f, indent=2, default=str)

        f.write("\n]\n")

if __name__ == '__main__':
    main()
