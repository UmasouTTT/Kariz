#!/usr/bin/python2.7

import boto.s3.connection



#Kaizen
access_key = "XJ5LS82583HOMDMEHZ0M"
secret_key = "eD3iAZsjSXkNbhDASjo80brVWNqINPKHW2fck3eT"
endpoint_url="https://kaizen.massopen.cloud"

conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host='192.168.35.41', port=80,
        is_secure=False, calling_format=boto.s3.connection.OrdinaryCallingFormat(),
       )


bucket = conn.create_bucket('data')
for bucket in conn.get_all_buckets():
    print "{name} {created}".format(
        name=bucket.name,
        created=bucket.creation_date,
    )
