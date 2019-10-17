#!/usr/bin/env python

import boto3
import asyncio

import datetime
import dateutil
import os
import json


tzutc = dateutil.tz.tz.tzutc()

class Prefix():
    def __init__(self, data=None, key=""):
        if data is None:
            self.number_objects = 0
            self.size = 0
            self.oldest = datetime.datetime.now(tz=tzutc)
            self.newest = datetime.datetime(year=1990, month=1, day=1).astimezone(tzutc)
            self.key = key
            self.breakdown = {}
        else:
            self.number_objects = 1
            self.size = data['Size']
            self.oldest = data['LastModified']
            self.newest = data['LastModified']
            self.key = data['Key']
            self.breakdown = {
                data['StorageClass']: data['Size']
            }

    def count(self, data):
        self.number_objects = self.number_objects + 1
        self.size = self.size + data['Size']
        self.oldest = min(self.oldest, data['LastModified'])
        self.newest = max(self.newest, data['LastModified'])
        self.breakdown[data['StorageClass']] = (
            self.breakdown.get(data['StorageClass'], 0) + data['Size']
        )

    def __add__(self, other):
        self.number_objects = self.number_objects + other.number_objects
        self.size = self.size + other.size
        self.oldest = min(self.oldest, other.oldest)
        self.newest = max(self.newest, other.newest)
        for storage_tier in (set(self.breakdown.keys()).union(set(other.breakdown.keys()))):
            self.breakdown[storage_tier] = (
                self.breakdown.get(storage_tier, 0) + other.breakdown.get(storage_tier, 0)
            )
        return self


class S3Counter():
    def __init__(self, prefix='', separator='/', depth=-1):
        # Start with basic counter
        self.separator = separator
        self.prefix = prefix
        self.depth = depth
        self.current_prefix = separator + prefix
        self.counters = [Prefix(key=prefix)]

    def report(self, counter):
        # Report a counter which has finished counting
        print("{size:>16}B  N: {count:>13} {key:>60}".format(
                size=counter.size, count=counter.number_objects, key=counter.key))


    def _compare_prefixes(self, l_prefix, r_prefix):
        # Check if prefixes are 'equal' to the given depth
        l_prefix_parts = (self.separator + l_prefix.lstrip(self.separator)).split(self.separator)
        r_prefix_parts = (self.separator + r_prefix.lstrip(self.separator)).split(self.separator)

        if self.depth >= 0:
            return l_prefix_parts[0:min(len(l_prefix_parts), self.depth)] == r_prefix_parts[0:min(len(r_prefix_parts), self.depth)]
        else:
            l_prefix_parts == r_prefix_parts


    def count(self, data_object):
        # Make sure that the correct counters are being held
        prefix_dir = (self.separator + data_object['Key']).rsplit(self.separator, maxsplit=1)[0]

        if self.depth >= 0 and data_object['Key'].count('/') < self.depth:
            # Show this object in the totals
            # print('Displaying object {}.'.format(data_object['Key']))
            self.report(Prefix(data=data_object))
        elif self._compare_prefixes(self.current_prefix, prefix_dir):
            # Don't show the object just count it
            # print('Counting object {}.'.format(data_object['Key']))
            self.counters[-1].count(data_object)
        else:
            # Current prefix is not correct, adjust it
            print('Adjusting prefixes. Current {}, new {}'.format(self.current_prefix, prefix_dir))
            prefix_increment = ''
            index = 0
            for prefix_part in prefix_dir.split(self.separator):
                prefix_increment = (prefix_increment + prefix_part + self.separator).lstrip(self.separator)
                if len(self.counters) > index and not self.counters[index].key == prefix_increment:
                    # Path from this point does not exist in the new path
                    while len(self.counters) > index:
                        counter = self.counters.pop()
                        # Add totals to next counter
                        self.counters[-1] = self.counters[-1] + counter
                        # Report totals for this prefix
                        self.report(counter)
                if len(self.counters) <= index:
                    # Create a new counter for this path
                    if data_object['Key'][-1] == self.separator:
                        # print('Adding new marker object {}.'.format(prefix_increment))
                        self.counters.append(Prefix(data=data_object))
                    else:
                        # print('Adding new prefix {}.'.format(prefix_increment))
                        self.counters.append(Prefix(key=prefix_increment))
                    # print('Markers: {}'.format(self.counters))
                self.current_prefix = self.separator + prefix_increment.lstrip(self.separator).rstrip(self.separator)
                index = index + 1


    def count_list(self, data_list):
        if not data_list:
            return
        prefix_dir_end = (self.separator + data_list[-1]['Key']).rsplit(self.separator, maxsplit=1)[0]
        if self._compare_prefixes(self.current_prefix, prefix_dir_end):
            # All items on this page are in the current prefix. Do a fast count.
            for data_object in data_list:
                self.counters[-1].count(data_object)
        else:
            # Items are a combination of prefixes. Check all prefixes while counting
            for data_object in data_list:
                self.count(data_object)


def s3_disk_usage( 
            Bucket, Depth=float('Inf'), Delimiter="/", Prefix="", #flatten_large_results=True,
            client=boto3.client('s3')
        ):
    # Calculate disk usage within S3 and report back to parent
    counter = S3Counter(prefix=Prefix, depth=Depth, separator=Delimiter)
    try:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix) # Delimiter=Delimiter

        for page in page_iterator:
            # Do something with the contents of this prefix
            # Deal with the files
            counter.count_list(page.get('Contents', []))
    except Exception as e:
        print("Exception counting objects in s3://{}/{}".format(Bucket, Prefix))
        print("Exception: {}".format(e))
        raise(e)


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
    # parser.add_argument("--truncate", type=bool, help="Summarise keys with over 1000 results?", default=True)
    parser.add_argument("--file", "-f", type=str, help="File name to output data to", default="")

    args = parser.parse_args()
    if args.depth <= -3:
        depth = float('Inf')
    else:
        depth = args.depth

    client = boto3.client('s3')
    # if args.file:
    #     output_file = args.file
    # else:
    #     output_file = os.devnull
    # append_object = False
    
    # with open(output_file, 'w') as f:
    #     f.write("[\n")
        # for statistic in s3_disk_usage( 
    s3_disk_usage( 
        Bucket=args.bucket,
        Depth=depth,
        Delimiter=args.delimiter,
        Prefix=args.prefix,
        # flatten_large_results=True,
        client=client
    )
        #     # Write to stdout
        #     if args.human:
        #         size = human_bytes(statistic['Size'])
        #         number = human_bytes(statistic['N'], base=10)
        #     else:
        #         size = statistic['Size']
        #         number = statistic['N']
        #     print("b: {PrintSize:>16}B N: {PrintNumber:>13} {Key:>60}   O: {Oldest:%Y-%m-%d} N: {Newest:%Y-%m-%d}".format(
        #         PrintSize=size, PrintNumber=number, **statistic))
        #     # Write to output file
        #     if append_object:
        #         f.write(",\n")
        #     else:
        #         append_object = True
        #     json.dump(statistic, f, indent=2, default=str)

        # f.write("\n]\n")

if __name__ == '__main__':
    main()
