import s3fs

from risk_backtesting.filesystems import FILESYSTEM_TYPE_S3, FILESYSTEM_TYPE_LOCAL
from risk_backtesting.writers.local_writer import LocalWriter
from risk_backtesting.writers.s3_writer import S3Writer
from risk_backtesting.writers.writer import Writer


def create_writer(writer_type: str, filesystem: s3fs.S3FileSystem) -> Writer:
    if FILESYSTEM_TYPE_LOCAL == writer_type:
        return LocalWriter()
    elif FILESYSTEM_TYPE_S3 == writer_type:
        return S3Writer(filesystem)
    else:
        raise ValueError(f"Unknown Write type {writer_type}")
