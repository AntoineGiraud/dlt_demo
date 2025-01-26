import dlt
from dlt.sources.sql_database import sql_database

source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
    table_names=[
        "family",
    ],
)

pipeline = dlt.pipeline(
    pipeline_name="dlt_demo",
    # destination="duckdb",
    destination="filesystem",
    # dataset_name="sql_data",
    dataset_name="rfam_parquet",
    # dev_mode=True, # pour isoler :)
)


load_info = pipeline.run(
    source,
    # if file format one ?
    loader_file_format="parquet",
    # table_format="iceberg",  # ça bug en local
)
print(load_info)


family = pipeline.dataset(dataset_type="default").family.df()
print(f"family nb cols: {family.columns.size}")
