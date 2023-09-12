import glob
import boto3
import awswrangler as wr
import smart_open
from joblib import register_store_backend
from joblib._store_backends import StoreBackendBase, StoreBackendMixin, CacheItemInfo


class S3StoreBackend(StoreBackendBase, StoreBackendMixin):
    _item_exists = staticmethod(wr.s3.does_object_exist)

    def _open_item(self, location, mode):
        return smart_open.open(location, mode, transport_params=self.transport_params)

    def _move_item(self, src, dst):
        # awswrangler only includes a fancy move/rename method that actually
        # makes it pretty hard to just do a simple move.
        src, dst = [smart_open.s3.parse_uri(x) for x in (src, dst)]
        self.client.copy_object(
            Bucket=dst["bucket_id"],
            Key=dst["key_id"],
            CopySource=f"{src['bucket_id']}/{src['key_id']}",
        )
        self.client.delete_object(Bucket=src["bucket_id"], Key=src["key_id"])

    def create_location(self, location):
        # Actually don't do anything. There are no locations on S3.
        pass

    def clear_location(self, location):
        # Recursive delete.
        wr.s3.delete_objects(glob.escape(location))

    def get_items(self):
        return [
            CacheItemInfo(
                key,
                item["ContentLength"],
                # This is supposed to be an access date, but it only gets used
                # this way for LRU caching, so it's not a big deal to use the
                # modified time instead.
                item["LastModified"],
            )
            for key, item in wr.s3.describe_objects(self.location).items()
        ]

    def configure(self, location, verbose=0, backend_options={}):
        self.verbose = verbose
        self.compress = backend_options.get("compress", False)

        self.mmap_mode = backend_options.get("mmap_mode")
        if self.mmap_mode is not None:
            raise ValueError("impossible to mmap on S3.")

        if not location.startswith("s3://"):
            raise ValueError("location must be an s3:// URI")

        # Theoretically we should validate that the bucket exists and we have
        # permission to write files to it, but for the POC I'm lazy.
        self.location = location

        # Get transport params for smart_open out of awswrangler, assuming that
        # awswrangler has been configured by the user.
        self.client = boto3.Session().client(
            "s3", endpoint_url=wr.config.s3_endpoint_url
        )

        self.transport_params = dict(client=self.client)


def install():
    register_store_backend("s3", S3StoreBackend)
