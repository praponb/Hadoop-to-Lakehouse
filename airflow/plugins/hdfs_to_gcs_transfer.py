"""Stream HDFS partitions into the GCS landing zone over WebHDFS.

Used by the `distcp_hdfs_to_gcs` task. Files are streamed chunk by chunk
(HDFS read -> GCS resumable upload) without touching local disk, and each
upload is checked against the MD5 that GCS computes server-side. Re-running a
partition overwrites the same object names, so the transfer is idempotent.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import posixpath

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # multiple of 256 KiB, as GCS resumable uploads require


class HdfsToGcsBridge:
    """Copy every file of one HDFS directory to a GCS prefix and return a manifest."""

    def __init__(
        self,
        namenode_url: str,
        landing_bucket: str,
        gcp_project: str | None = None,
        hdfs_user: str = "root",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        hdfs_client=None,
        gcs_client=None,
    ):
        self.bucket_name = landing_bucket.removeprefix("gs://").strip("/")
        self.chunk_size = chunk_size
        if hdfs_client is None:
            from hdfs import InsecureClient

            hdfs_client = InsecureClient(namenode_url, user=hdfs_user)
        if gcs_client is None:
            from google.cloud import storage

            gcs_client = storage.Client(project=gcp_project)
        self.hdfs = hdfs_client
        self.bucket = gcs_client.bucket(self.bucket_name)

    def list_partition_files(self, hdfs_dir: str) -> list[str]:
        """Data files in `hdfs_dir`, skipping sub-directories and _SUCCESS/.hidden markers."""
        return sorted(
            name
            for name, status in self.hdfs.list(hdfs_dir, status=True)
            if status["type"] == "FILE" and not name.startswith(("_", "."))
        )

    def transfer_file(self, hdfs_path: str, object_name: str) -> tuple[int, str]:
        """Stream one file; return (bytes, md5 hex). Raises if GCS's MD5 disagrees."""
        md5 = hashlib.md5()
        size = 0
        blob = self.bucket.blob(object_name, chunk_size=self.chunk_size)
        with self.hdfs.read(hdfs_path, chunk_size=self.chunk_size) as reader, blob.open("wb") as writer:
            for chunk in reader:
                md5.update(chunk)
                size += len(chunk)
                writer.write(chunk)

        blob.reload()
        expected = base64.b64encode(md5.digest()).decode()
        if blob.md5_hash != expected:
            raise IOError(f"MD5 mismatch for gs://{self.bucket_name}/{object_name}: hdfs={expected} gcs={blob.md5_hash}")
        if blob.size is not None and blob.size != size:
            raise IOError(f"Size mismatch for gs://{self.bucket_name}/{object_name}: hdfs={size} gcs={blob.size}")
        return size, md5.hexdigest()

    def transfer_partition(self, hdfs_dir: str, gcs_prefix: str) -> dict:
        """Copy all files under `hdfs_dir` to gs://<bucket>/<gcs_prefix>/ and return the manifest."""
        files = self.list_partition_files(hdfs_dir)
        if not files:
            raise FileNotFoundError(f"No data files found in HDFS directory {hdfs_dir}")

        manifest = {
            "source": hdfs_dir,
            "destination": f"gs://{self.bucket_name}/{gcs_prefix.strip('/')}/",
            "files_transferred": 0,
            "total_bytes": 0,
            "md5_hashes": {},
        }
        for name in files:
            object_name = posixpath.join(gcs_prefix.strip("/"), name)
            size, md5_hex = self.transfer_file(posixpath.join(hdfs_dir, name), object_name)
            manifest["files_transferred"] += 1
            manifest["total_bytes"] += size
            manifest["md5_hashes"][f"gs://{self.bucket_name}/{object_name}"] = md5_hex
            log.info("Copied %s/%s -> gs://%s/%s (%d bytes, md5 %s)", hdfs_dir, name, self.bucket_name, object_name, size, md5_hex)

        log.info("Transferred %d file(s), %d bytes to %s", manifest["files_transferred"], manifest["total_bytes"], manifest["destination"])
        return manifest
