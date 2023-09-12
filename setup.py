import setuptools

setuptools.setup(
    name="joblib_awswrangler",
    author="Alex Spaeth",
    author_email="atspaeth@ucsc.edu",
    description="joblib.Memory backend for S3 storage",
    requires=["joblib", "awswrangler", "smart_open"],
    version="0.0.1",
)
