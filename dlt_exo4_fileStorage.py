import dlt
from dlt.sources.filesystem import filesystem, read_parquet

filesystem_resource = filesystem(
    bucket_url="./data",
    file_glob="**/*.parquet",
)
filesystem_pipe = filesystem_resource | read_parquet()

# We load the data into the table_name table
pipeline = dlt.pipeline(
    pipeline_name="dlt_demo",
    destination="duckdb",
    dataset_name="fileSystem",
)
load_info = pipeline.run(filesystem_pipe.with_name("userdata"))
print(load_info)


userdata = pipeline.dataset(dataset_type="default").userdata.df()
print(f"userdata nb cols: {userdata.columns.size}")
