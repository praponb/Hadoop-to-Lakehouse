"""HdfsToGcsBridge against in-memory fakes of the hdfs and google-cloud-storage clients."""
import base64
import hashlib
import io
from contextlib import contextmanager

import pytest

from hdfs_to_gcs_transfer import HdfsToGcsBridge


class FakeHdfs:
    def __init__(self, files):
        self.files = files  # {path: bytes}

    def list(self, hdfs_dir, status=False):
        entries = [("nested", {"type": "DIRECTORY"}), ("_SUCCESS", {"type": "FILE"})]
        prefix = hdfs_dir.rstrip("/") + "/"
        entries += [(p[len(prefix):], {"type": "FILE"}) for p in self.files if p.startswith(prefix)]
        return entries

    @contextmanager
    def read(self, path, chunk_size):
        data = self.files[path]
        yield (data[i:i + chunk_size] for i in range(0, len(data), chunk_size))


class FakeBlob:
    def __init__(self, store, name, corrupt=False):
        self.store, self.name, self.corrupt = store, name, corrupt
        self.md5_hash = self.size = None

    @contextmanager
    def open(self, mode):
        buf = io.BytesIO()
        yield buf
        self.store[self.name] = buf.getvalue() + (b"!" if self.corrupt else b"")

    def reload(self):
        data = self.store[self.name]
        self.md5_hash = base64.b64encode(hashlib.md5(data).digest()).decode()
        self.size = len(data)


class FakeGcs:
    def __init__(self, corrupt=False):
        self.store, self.corrupt, self.bucket_name = {}, corrupt, None

    def bucket(self, name):
        self.bucket_name = name
        return self

    def blob(self, name, chunk_size=None):
        return FakeBlob(self.store, name, self.corrupt)


FILES = {
    "/legacy/erp/orders/ingest_date=2026-10-14/orders_part_0.csv": b"a,b\n" * 1000,
    "/legacy/erp/orders/ingest_date=2026-10-14/orders_part_1.csv": b"c,d\n" * 10,
}


def make_bridge(gcs):
    return HdfsToGcsBridge("http://namenode:9870", "gs://landing-bucket", chunk_size=256, hdfs_client=FakeHdfs(FILES), gcs_client=gcs)


def test_transfer_partition_streams_all_files_and_builds_manifest():
    gcs = FakeGcs()
    manifest = make_bridge(gcs).transfer_partition("/legacy/erp/orders/ingest_date=2026-10-14", "erp/orders/ingest_date=2026-10-14")

    assert gcs.bucket_name == "landing-bucket"
    assert manifest["files_transferred"] == 2
    assert manifest["total_bytes"] == sum(len(v) for v in FILES.values())
    assert manifest["destination"] == "gs://landing-bucket/erp/orders/ingest_date=2026-10-14/"
    assert manifest["md5_hashes"] == {
        f"gs://landing-bucket/erp/orders/ingest_date=2026-10-14/{p.rsplit('/', 1)[1]}": hashlib.md5(v).hexdigest()
        for p, v in FILES.items()
    }
    assert gcs.store["erp/orders/ingest_date=2026-10-14/orders_part_0.csv"] == FILES["/legacy/erp/orders/ingest_date=2026-10-14/orders_part_0.csv"]


def test_transfer_is_idempotent():
    gcs = FakeGcs()
    bridge = make_bridge(gcs)
    first = bridge.transfer_partition("/legacy/erp/orders/ingest_date=2026-10-14", "erp/orders/ingest_date=2026-10-14")
    second = bridge.transfer_partition("/legacy/erp/orders/ingest_date=2026-10-14", "erp/orders/ingest_date=2026-10-14")
    assert first == second
    assert len(gcs.store) == 2


def test_md5_mismatch_raises():
    with pytest.raises(IOError, match="MD5 mismatch"):
        make_bridge(FakeGcs(corrupt=True)).transfer_partition("/legacy/erp/orders/ingest_date=2026-10-14", "erp/orders/x")


def test_empty_partition_raises():
    with pytest.raises(FileNotFoundError):
        make_bridge(FakeGcs()).transfer_partition("/legacy/erp/orders/ingest_date=2030-01-01", "erp/orders/x")
