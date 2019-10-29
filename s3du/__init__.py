#!/usr/bin/env python

import boto3
import asyncio

import datetime
import dateutil
import os
import json
import urllib
import gzip
import codecs
import csv


tzutc = dateutil.tz.tz.tzutc()


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
    if n == 0:
        return "{:d} {:>2}".format(size, power_labels[n])
    else:
        return "{:.1f} {:>2}".format(size, power_labels[n])


class Prefix():
    __slots__ = ('number_objects', 'size', 'oldest', 'newest', 'key', 'breakdown')
    def __init__(self, data=None, key="", number_objects=0, size=0, breakdown={},
            oldest=datetime.datetime.now(tz=tzutc), newest=datetime.datetime(year=1990, month=1, day=1).astimezone(tzutc)):
        if data is None:
            self.number_objects = number_objects
            self.size = size
            self.oldest = oldest
            self.newest = newest
            self.key = key
            self.breakdown = breakdown
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

    def depth(self, separator='/'):
        return(len(self.key.rstrip(separator).split(separator)))

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

    def compare(self, other, depth, separator='/'):
        # Check if prefixes are 'equal' to the given depth
        l_prefix_parts = self.key.split(separator)[0:-1]
        r_prefix_parts = other.key.split(separator)[0:-1]

        if depth == 0:
            return True
        elif depth > 0:
            # print(l_prefix_parts[0:min(len(l_prefix_parts), depth)])
            # print(r_prefix_parts[0:min(len(r_prefix_parts), depth)])
            return l_prefix_parts[0:min(len(l_prefix_parts), depth)] == r_prefix_parts[0:min(len(r_prefix_parts), depth)]
            # print(rval)
            # return rval
        else:
            return l_prefix_parts == r_prefix_parts


class S3Counter():
    def __init__(self, prefix='', separator='/', depth=-1, limit=20, file_name="", human=False):
        # Start with basic counter
        self.separator = separator
        self.prefix = prefix
        self.depth = depth
        self.limit = limit
        self.counters = [Prefix(key=prefix)]
        self.human = human
        self.reports_at_depth = 0

        self.file_name = file_name
        if self.file_name:
            self.output_file = open(file_name, 'w')

    def __del__(self):
        if self.file_name:
            self.output_file.close()

    def report(self, counter):
        # Report a counter which has finished counting
        if counter.key == '':
            counter.key = '.'
        if self.human:
            print("{size:>16}B  {count:>13} {key:>60}  {oldest:%Y-%m-%d} {newest:%Y-%m-%d}".format(
                    size=human_bytes(counter.size), count=human_bytes(counter.number_objects, base=10), key=counter.key,
                    oldest=counter.oldest, newest=counter.newest))
        else:
            print("{size:>16}   {count:>13} {key:>60}  {oldest:%Y-%m-%d} {newest:%Y-%m-%d}".format(
                    size=counter.size, count=counter.number_objects, key=counter.key,
                    oldest=counter.oldest, newest=counter.newest))

        if self.file_name: 
            self.output_file.write('{{"N":{number_objects}, "size":{size}, "key":"{key}", "oldest":"{oldest}", "newest":"{newest}", "breakdown":{breakdown_str}}}\n'.format(
                breakdown_str=json.dumps(counter.breakdown),
                number_objects=counter.number_objects,
                size=counter.size,
                key=counter.key,
                oldest=counter.oldest,
                newest=counter.newest
            ))

    def report_omission(self):
        print("                         additional objects under \"{}\" omitted...".format(
            self.counters[-1].key
        ))

    def _pop_counter(self):
        counter = self.counters.pop()
        # Add totals to next counter
        if self.counters:
            self.counters[-1] = self.counters[-1] + counter
        # Report totals for this prefix
        self.report(counter)

    def _set_prefix(self, key):
        # Set the markers to the given object

        prefix_dir = (self.separator + key.lstrip(self.separator)).rsplit(self.separator, maxsplit=1)[0]
        prefix_parts = prefix_dir.split(self.separator)
        prefix_parts = prefix_parts[1:min(self.depth + 1, len(prefix_parts))] # +1 to include .
        prefix_increment = ''
        # print('Adjusting prefixes. Current {}, new {}, key: {}'.format(self.counters[-1].key, prefix_dir.lstrip(self.separator), key))
        for index, prefix_part in enumerate(prefix_parts):
            prefix_increment = (prefix_increment + prefix_part + self.separator) #.rstrip(self.separator)
            counter_index = index + 1
            # print("index: {}, {}, len: {}".format(counter_index, prefix_increment, len(self.counters)))
            if len(self.counters) > counter_index and not self.counters[counter_index].key == prefix_increment:
                # Path from this point does not exist in the new path
                self.reports_at_depth = 0
                while len(self.counters) > counter_index:
                    self._pop_counter()
            if len(self.counters) <= counter_index:
                self.reports_at_depth = 0
                # Create a new counter for this path
                # print('Adding new prefix {}.'.format(prefix_increment))
                self.counters.append(Prefix(key=prefix_increment))
            # print(self.counters)
        while len(self.counters) > len(prefix_parts) +1:
            # print("pop! {}".format(prefix_parts))
            self._pop_counter()
            # print(self.counters)


    def finalise(self):
        while self.counters:
            self._pop_counter()


    def count_list(self, data_list):
        if not data_list:
            return
        fast_count = self.counters[-1].compare(data_list[-1], self.depth, self.separator)
        # print(fast_count)
        if not fast_count:
            # Try setting the prefix to the front object
            self._set_prefix(data_list[0].key)
            fast_count = self.counters[-1].compare(data_list[-1], self.depth, self.separator)
        if fast_count:
            # All items on this page are in the current prefix. Do a fast count until the end.
            # print("Fast count {}".format(data_list[-1].key))
            for data_object in data_list:
                self.counters[-1] = self.counters[-1] + data_object
        else:
            # Don't know about how the objects on this page match up, indexes will need to be changed.
            # print("Slow count {}".format(data_list[-1].key))
            if len(data_list) > 8:
                # B chop the list
                pivot = int(len(data_list) / 2)
                self.count_list(data_list[0:pivot])
                self.count_list(data_list[pivot:])
            else:
                # List is small, count the items
                for data_object in data_list:
                    if not self.counters[-1].compare(data_object, self.depth, self.separator):
                        self._set_prefix(data_object.key)
                    self.counters[-1] = self.counters[-1] + data_object
                    # print("{}: depth: {}".format(data_object.key, data_object.depth(separator=self.separator)))
                    if self.reports_at_depth < self.limit and data_object.depth(separator=self.separator) <= self.depth:
                        self.report(data_object)
                        self.reports_at_depth = self.reports_at_depth + 1
                        if self.reports_at_depth == self.limit:
                            self.report_omission()


def peek_inventory_data_file(Bucket, Key, fields, client):
    entry = next(read_inventory_data_file(
        Bucket=Bucket, Key=Key, fields=fields, client=client, page_size=1))
    return entry[0].key


def read_inventory_data_file(Bucket, Key, fields, client, page_size=500):
    # Read an inventory data file from S3 and bunch the data up into pages
    data_object = client.get_object(Bucket=Bucket, Key=Key)
    stream = data_object['Body']
    if 'gzip' in data_object['ContentType']:
        stream = gzip.GzipFile(fileobj=stream)
    stream = codecs.iterdecode(stream, encoding='utf-8')
    reader = csv.reader(stream)
    page = []
    idx_size = fields.index("Size")
    idx_key = fields.index("Key")
    idx_storageclass = fields.index("StorageClass")
    idx_time = fields.index("LastModifiedDate")

    for line in reader:
        try:
            dt = datetime.datetime.strptime(line[idx_time], "%Y-%m-%dT%H:%M:%S.%f%z")
            size = line[idx_size]
            page.append(
                Prefix(
                    key=line[idx_key],
                    number_objects=1,
                    size=(int(size) if size else 0),
                    breakdown={line[idx_storageclass]: (int(size) if size else 0)},
                    oldest=dt,
                    newest=dt
            ))
            if len(page) >= page_size:
                yield page
                page = []
        except Exception as e:
            print("Could not parse line {} with fields {}.".format(line, fields))
            raise e
    yield page


def s3_disk_usage_from_inventory( 
        InventoryLocation, Depth=float('Inf'), Delimiter="/", Prefix="", MaxObjectsToDisplay=10, File="",
        Human=False, client=boto3.client('s3'), 
    ):
    try:
        inventory_location = urllib.parse.urlparse(InventoryLocation)
        if not inventory_location.scheme == 's3':
            raise("Unsupported scheme: {}".format(inventory_location.scheme))
        inventory_bucket = inventory_location.netloc
    except Exception as e:
        raise Exception("An inventory URL must be an S3 location in the format s3://mybucket/myprefix/sourcebucket/2019-10-27T04-00Z/. {}".format(e))

    try:
        manifest_key = (inventory_location.path.strip('/') + '/manifest.json')
        manifest_object = client.get_object(Bucket=inventory_bucket, Key=manifest_key)
    except Exception as e:
        print("Could not access manifest key {}".format(manifest_key))
        raise e

    manifest = json.load(manifest_object['Body'])
    if not manifest['fileFormat']:
        raise Exception("Unsupported report format {}. Supported formats: ['CSV'].".format(manifest['fileFormat']))
    schema = manifest['fileSchema']
    fields = [field.strip() for field in schema.split(',')]

    # Start counting objects
    counter = S3Counter(
        prefix=Prefix,
        depth=Depth,
        separator=Delimiter,
        limit=MaxObjectsToDisplay,
        file_name=File,
        human=Human
    )

    # For some reason the file orders are screwed up...
    for fileno, data_file in enumerate(manifest['files']):
        data_file['first_key'] = peek_inventory_data_file(
            Bucket=inventory_bucket, Key=data_file['key'], fields=fields, client=client
        )

    manifest['files'] = [x for _,x in sorted(
        zip([obj['first_key'] for obj in manifest['files']],manifest['files']))]

    for fileno, data_file in enumerate(manifest['files']):
        # print("Opening file ({} / {}): {}".format(fileno, len(manifest['files']), data_file))
        for page in read_inventory_data_file(Bucket=inventory_bucket, Key=data_file['key'], fields=fields, client=client):
            counter.count_list(page)
    counter.finalise()


async def count_page(counter, page):
    counter.count_list([Prefix(data=obj) for obj in page.get('Contents', []) ])


async def s3_disk_usage( 
            Bucket, Depth=float('Inf'), Delimiter="/", Prefix="", MaxObjectsToDisplay=10, File="",
            Human=False, client=boto3.client('s3')
        ):
    # Calculate disk usage within S3 and report back to parent
    counter = S3Counter(
        prefix=Prefix,
        depth=Depth,
        separator=Delimiter,
        limit=MaxObjectsToDisplay,
        file_name=File,
        human=Human
    )
    try:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix) #, Delimiter=Delimiter)

        last_page = {"Contents": []}
        task = asyncio.create_task(count_page(counter, last_page))
        for page in page_iterator:
            # Count the files
            await task
            last_page = page
            # print(last_page)
            task = asyncio.create_task(count_page(counter, last_page))
            # Measured speed difference between synchronous and asynchronous methods was marginal..
        await task
        counter.finalise()
    except Exception as e:
        print("Exception counting objects in s3://{}/{}".format(Bucket, Prefix))
        print("Exception: {}".format(e))
        raise(e)


def main():
    try:
        import argparse 
    except ImportError:
        print("ERROR: You are running Python < 2.7. Please use pip to install argparse:   pip install argparse")

    parser = argparse.ArgumentParser(add_help=True, description="Display S3 usage by storage tier")
    parser.add_argument("--depth", "-d", type=int, help="Maximum depth (0 by default)", default=-3)
    parser.add_argument("--human", help="Human readable sizes", default=False, action='store_true')
    parser.add_argument("--bucket", type=str, help="S3 bucket name. Not required if using inventory reports.")
    parser.add_argument("--prefix", type=str, help="S3 bucket prefix", default="")
    parser.add_argument("--delimiter", type=str, help="S3 bucket delimiter", default="/")
    parser.add_argument("--truncate", type=int, help="Truncate list over N results?", default=25)
    parser.add_argument("--file", "-f", type=str, help="File name to output data to (in jsonl format)", default="")
    parser.add_argument("--inventory-url", type=str, default="",
        help="S3 URL to an S3 inventory report (e.g s3://mybucket/myprefix/sourcebucket/2019-10-27T04-00Z/)")

    args = parser.parse_args()
    if args.depth <= -3:
        depth = float('Inf')
    else:
        depth = args.depth

    client = boto3.client('s3')
    if args.inventory_url:
        s3_disk_usage_from_inventory(
            InventoryLocation=args.inventory_url,
            Depth=depth,
            Delimiter=args.delimiter,
            Prefix=args.prefix,
            MaxObjectsToDisplay=args.truncate,
            File=args.file,
            Human=args.human,
            client=client
        )
    else:
        asyncio.run(s3_disk_usage( 
            Bucket=args.bucket,
            Depth=depth,
            Delimiter=args.delimiter,
            Prefix=args.prefix,
            MaxObjectsToDisplay=args.truncate,
            File=args.file,
            Human=args.human,
            client=client
        ))

if __name__ == '__main__':
    main()
