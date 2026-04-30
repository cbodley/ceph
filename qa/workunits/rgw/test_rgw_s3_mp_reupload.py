import boto3
import botocore.exceptions
import sys
import os
import subprocess
from common import boto3_client

#boto3.set_stream_logger(name='botocore')

# handles two optional system arguments:
#   <bucket-name> : default is "bkt134"
#   <0 or 1>      : 0 -> upload aborted, 1 -> completed; default is completed

if len(sys.argv) >= 2:
    bucket_name = sys.argv[1]
else:
    bucket_name = "bkt314738362229"
print("bucket nams is %s" % bucket_name)

complete_mpu = True
if len(sys.argv) >= 3:
    complete_mpu = int(sys.argv[2]) > 0

versioned_bucket = False
if len(sys.argv) >= 4:
    versioned_bucket = int(sys.argv[3]) > 0

access_key = os.environ['RGW_ACCESS_KEY']
secret_key = os.environ['RGW_SECRET_KEY']

client = boto3_client(access_key, secret_key)
client.create_bucket(Bucket=bucket_name)

if versioned_bucket:
    res = client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
        'MFADelete': 'Disabled',
        'Status': 'Enabled'}
        )

key = "mpu_test4"
nparts = 2
ndups = 11
do_reupload = True

part_path = "/tmp/mp_part_5m"
subprocess.run(["dd", "if=/dev/urandom", "of=" + part_path, "bs=1M", "count=5"], check=True)

f = open(part_path, 'rb')

res = client.create_multipart_upload(Bucket=bucket_name, Key=key)
mpu_id = res["UploadId"]

print("start UploadId=%s" % (mpu_id))

parts = []
parts2 = []

for ix in range(0,nparts):
    part_num = ix + 1
    f.seek(0)
    res = client.upload_part(Body=f, Bucket=bucket_name, Key=key,
                             UploadId=mpu_id, PartNumber=part_num)
    # save
    etag = res['ETag']
    part = {'ETag': etag, 'PartNumber': part_num}
    print("phase 1 uploaded part %s" % part)
    parts.append(part)

if do_reupload:
    # just re-upload part 1
    part_num = 1
    for ix in range(0,ndups):
        f.seek(0)
        res = client.upload_part(Body=f, Bucket=bucket_name, Key=key,
                                UploadId=mpu_id, PartNumber=part_num)
        etag = res['ETag']
        part = {'ETag': etag, 'PartNumber': part_num}
        print ("phase 2 uploaded part %s" % part)

        # save
        etag = res['ETag']
        part = {'ETag': etag, 'PartNumber': part_num}
        parts2.append(part)

if complete_mpu:
    print("completing multipart upload, parts=%s" % parts)
    res = client.complete_multipart_upload(
        Bucket=bucket_name, Key=key, UploadId=mpu_id,
        MultipartUpload={'Parts': parts})
else:
    print("aborting multipart upload, parts=%s" % parts)
    res = client.abort_multipart_upload(
        Bucket=bucket_name, Key=key, UploadId=mpu_id)

# clean up
subprocess.run(["rm", "-f", part_path], check=True)
