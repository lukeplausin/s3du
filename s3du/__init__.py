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
    def __init__(self, prefix='', separator='/', depth=-1, limit=20, file_name="", human=False):
        # Start with basic counter
        self.separator = separator
        self.prefix = prefix
        self.depth = depth
        self.limit = limit
        self.current_prefix = separator + prefix
        self.counters = [Prefix(key=prefix)]
        self.human = human

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
                breakdown_str=json.dumps(counter.breakdown), **vars(counter)))

        # f.write("\n]\n")
    def report_omission(self):
        print("                         additional objects under \"{}\" omitted...".format(
            self.counters[-1].key
        ))

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

        if self.depth >= 0 and data_object['Key'].count(self.separator) >= self.depth:
            # Don't show the object just count it (out of depth)
            self.counters[-1].count(data_object)
        elif self._compare_prefixes(self.current_prefix, prefix_dir):
            # Current prefix is set correctly, display this object
            if self.counters[-1].number_objects >= self.limit:
                # Do not display this object, just count it (truncate long list)
                self.counters[-1].count(data_object)
            else:
                # Display and count object
                counter = Prefix(data=data_object)
                self.counters[-1] = self.counters[-1] + counter
                self.report(counter)
                if self.counters[-1].number_objects >= self.limit:
                    self.report_omission()
        else:
            # Current prefix is not correct, adjust it
            # print('Adjusting prefixes. Current {}, new {}'.format(self.current_prefix, prefix_dir))
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

    def finalise(self):
        while self.counters:
            counter = self.counters.pop()
            # Add totals to next counter
            if self.counters:
                self.counters[-1] = self.counters[-1] + counter
            # Report totals for this prefix
            self.report(counter)


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


def read_inventory_data_file(Bucket, Key, fields, client, page_size=500):
    # Read an inventory data file from S3 and bunch the data up into pages
    data_object = client.get_object(Bucket=Bucket, Key=Key)
    stream = data_object['Body']
    if 'gzip' in data_object['ContentType']:
        stream = gzip.GzipFile(fileobj=stream)
    stream = codecs.iterdecode(stream, encoding='utf-8')
    reader = csv.reader(stream)
    page = []
    types = [str for i in range(len(fields))]
    for i, field in enumerate(fields):
        if field == 'Size':
            types[i] = int
        elif field == 'LastModified':
            types[i] = dateutil.parser.parse
    for line in reader:
        page.append({
            fields[i]: types[i](data) for i, data in enumerate(line)
        })
        if len(page) >= page_size:
            yield page
            page = []
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

    manifest_object = client.get_object(Bucket=inventory_bucket, Key=(inventory_location.path.strip('/') + '/manifest.json'))
    manifest = json.load(manifest_object['Body'])
    if not manifest['fileFormat']:
        raise Exception("Unsupported report format {}. Supported formats: ['CSV'].".format(manifest['fileFormat']))
    schema = manifest['fileSchema']
    fields = [field.strip() for field in schema.split(',')]
    for i, field in enumerate(fields):
        if field == 'LastModifiedDate':
            fields[i] = 'LastModified'

    # Start counting objects
    counter = S3Counter(
        prefix=Prefix,
        depth=Depth,
        separator=Delimiter,
        limit=MaxObjectsToDisplay,
        file_name=File,
        human=Human
    )

    for data_file in manifest['files']:
        try:
            for page in read_inventory_data_file(Bucket=inventory_bucket, Key=data_file['key'], fields=fields, client=client):
                counter.count_list(page)
        except Exception as e:
            print("Failed to count data file {key}.".format(**data_file))


async def count_page(counter, page):
    counter.count_list(page.get('Contents', []))


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
        page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix) # Delimiter=Delimiter

        last_page = {"Contents": []}
        task = asyncio.create_task(count_page(counter, last_page))
        for page in page_iterator:
            # Count the files
            await task
            last_page = page
            task = asyncio.create_task(count_page(counter, last_page))
            # Measured speed difference between synchronous and asynchronous methods was marginal..
        await task
        await count_page(counter, page)
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
