import boto
import boto.s3.connection

access_key = '5b0d738e101946e2acee17e46e04fd26'
secret_key = 'c5feba0ca6d54cadab3e80a98d849f5d'

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 'swift-kaizen.massopen.cloud',
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

# create a bucket
bucket = conn.create_bucket('mahbuckat1')

# list all buckets
for bucket in conn.get_all_buckets():
        print "{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        )

