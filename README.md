# s3du
du - like utility for Amazon's S3 object storage service

# What does it do?

This utility counts disk space in S3 by path (and also optionally by storage tier).

# Why should I use this s3du instead of all the other ones?

- This version uses the low level client and paging to cut the number of requests to S3

- This version uses streaming so that the memory footprint is reasonable even, for huge buckets with small objects

- This s3du script can be imported into other python projects

```
from s3du import s3_disk_usage

s3_disk_usage(bucket="my_bucket")
```

# Future improvements

- Use multiple threads to count file sizes, cutting down waiting times when working with huge buckets
