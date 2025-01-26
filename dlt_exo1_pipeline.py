import dlt
import duckdb

# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]


# Set pipeline name, destination, and dataset name
pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination="duckdb",
    dataset_name="mydata",
)

# Run the pipeline with data and table name
load_info = pipeline.run(data, table_name="pokemon")

print(load_info)


# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

# --------------------------
# (2) dlt's sql_client

# Query data from 'pokemon' using the SQL client
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM pokemon") as cursor:
        data = cursor.df()

# Display the data
data
print(f"{data.columns:}")


# --------------------------
# (3) dlt datasets

dataset = pipeline.dataset(dataset_type="default")
poke = dataset.pokemon.df()

print(poke)
