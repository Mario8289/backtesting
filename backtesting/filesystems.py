from typing import Dict, Any

import boto3
import s3fs

FILESYSTEM_TYPE_LOCAL: str = "local"
FILESYSTEM_TYPE_S3: str = "s3"


def initialise_s3fs(auth: Dict[str, Dict[str, Any]], section: str) -> s3fs.S3FileSystem:
    if auth and section in auth:
        sect: Dict[str, Dict[str, Any]] = auth[section]
        return s3fs.S3FileSystem(
            key=sect["minio"]["key"],
            secret=sect["minio"]["secret"],
            client_kwargs={"endpoint_url": sect["minio"]["uri"]},
            config_kwargs={"signature_version": "s3v4"},
        )
    # the below is the original behaviour, but it doesn't make a lot of sense, so replacing it with None
    # return s3fs.S3FileSystem(anon=False)
    return None


def initialise_boto3(
        auth: Dict[str, Dict[str, Any]], section: str
):  # no idea what it returns
    if auth and section in auth:
        sect: Dict[str, Dict[str, Any]] = auth[section]
        return boto3.resource(
            FILESYSTEM_TYPE_S3,
            aws_access_key_id=sect["minio"]["key"],
            aws_secret_access_key=sect["minio"]["secret"],
            endpoint_url=sect["minio"]["uri"],
            region_name=sect["minio"].get("region", "us-east-1"),
        )
    # the below is the original behaviour, but it doesn't make a lot of sense, so replacing it with None
    # return boto3.resource(FILESYSTEM_TYPE_S3)
    return None
