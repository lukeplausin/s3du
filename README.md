# s3du
du - like utility for Amazon's S3 object storage service

# What does it do?

This utility counts disk space in S3 by path (and also optionally by storage tier).

# Why should I use this s3du instead of all the other ones?

- Uses the low level client and paging to cut the number of requests to S3, retrieving answers in a fast an inexpensive way.

- Uses streaming so that the memory footprint is reasonable even, for huge buckets containing billions of small objects.

- Has been packaged as a module for easy installation with pip, and with a cli entrypoint set up.

- Can be imported into other python projects:


```python
from s3du import s3_disk_usage

s3_disk_usage(bucket="my_bucket")
```

# Installation

`pip install git+https://github.com/lukeplausin/s3du.git`

# Usage

```bash
# Print help
s3du --help

# List common prefixes and object size totals in 'my-bucket', to a depth
# of two objects, under the 'my/file/location' prefix
s3du --bucket my-bucket --prefix my/file/location --depth 2

# Print entire output to screen in human readable format, and save data
# into a file called contents.json
s3du --bucket my-bucket --file contents.json --human

# Analyse a bucket using a delimiter other than the default /
s3du --bucket my-bucket --delimiter '\'
```

The format will look like this, the fields are size in bytes (b), number of files (N), file name, oldest file date (O), newest file date (N):
```bash
$ s3du --bucket my-bucket --depth 1
b:         59516837 N:           567                                                        dist/   O: 2019-07-02 N: 2019-07-02
b:           170032 N:             1                                            bootstrap.min.css   O: 2019-07-02 N: 2019-07-02
b:            58072 N:             1                                             bootstrap.min.js   O: 2019-07-02 N: 2019-07-02
b:             1262 N:             1                                                   index.html   O: 2019-07-02 N: 2019-07-02
b:             9610 N:             1                                                      list.js   O: 2019-07-02 N: 2019-07-02
b:         59755813 N:           571                                                            .   O: 2019-07-02 N: 2019-07-02
```

To customise the format, import the module into your code.

# Future improvements

- Use multiple threads to count file sizes, cutting down waiting times when working with huge buckets
- Sort by size, number of objects, projected cost
