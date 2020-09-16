#!/usr/bin/python2.7

import boto.s3.connection



#Kaizen
access_key = "4c3da79d02bb4a2e8f04495bff5203b2"
secret_key = "b7bd5b4abcd34ca8a94e93e8b76527f4"
endpoint_url="https://kaizen.massopen.cloud"

conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host='192.168.35.41', port=7480,
        is_secure=False, calling_format=boto.s3.connection.OrdinaryCallingFormat(),
       )


bucket = conn.create_bucket('alluxio')
for bucket in conn.get_all_buckets():
    print "{name} {created}".format(
        name=bucket.name,
        created=bucket.creation_date,
    )
