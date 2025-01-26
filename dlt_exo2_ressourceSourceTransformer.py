import requests
import dlt


@dlt.resource(table_name="github_events", write_disposition="replace")
def github_events():
    url = "https://api.github.com/orgs/dlt-hub/events"
    response = requests.get(url)
    yield response.json()


@dlt.resource(table_name="github_repos", write_disposition="replace")
def github_repos():
    url = "https://api.github.com/orgs/dlt-hub/repos"
    response = requests.get(url)
    yield from response.json()


@dlt.transformer(data_from=github_repos, table_name="github_stargazers")
def github_stargazers(repo):  # <--- Transformer receives one item at a time
    url = f"https://api.github.com/repos/dlt-hub/{repo['name']}/stargazers"
    response = requests.get(url)

    yield response.json()


@dlt.source()  # max_table_nesting=1
def src_github_data():
    return github_events, github_repos, github_stargazers


# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="dlt_demo",
    destination="duckdb",
    dataset_name="github",
)


# Run the pipeline using the defined resource
load_info = pipeline.run(src_github_data())
print(load_info)

# Query the loaded data from 'pokemon_api' table
tbl_repos = pipeline.dataset(dataset_type="default").github_repos.df()
tbl_events = pipeline.dataset(dataset_type="default").github_events.df()
tbl_stargazers = pipeline.dataset(dataset_type="default").github_stargazers.df()

print(f"repo nb cols: {tbl_repos.columns.size}")
print(f"events nb cols: {tbl_events.columns.size}")
print(f"stargazers nb cols: {tbl_stargazers.columns.size:}")
