"""The Github API templates provides a starting point to read data from REST APIs with REST Client helper"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional

import dlt

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.source(max_table_nesting=1)
def github_source(access_token: Optional[str] = dlt.secrets.value):
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(access_token) if access_token else None,
        paginator=HeaderLinkPaginator(),
    )

    @dlt.resource(table_name="github_repos", write_disposition="replace")
    def github_repos():
        for page in client.paginate("orgs/dlt-hub/repos"):
            yield from page

    @dlt.resource(table_name="github_events")
    def github_events():
        for page in client.paginate("orgs/dlt-hub/events"):
            yield page

    @dlt.transformer(
        table_name="github_pulls_comments",
        data_from=github_repos,
        write_disposition="merge",
        primary_key="id",
    )
    def github_pulls_comments(
        repo,
        cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01"),
    ):  # <--- Transformer receives one item at a time
        params = {"since": cursor_date.last_value}
        for page in client.paginate(
            f"repos/dlt-hub/{repo['name']}/pulls/comments",
            params=params,
        ):
            yield page

    @dlt.transformer(
        table_name="github_stargazers",
        data_from=github_repos,
        write_disposition="merge",
        primary_key="id",
    )
    def github_stargazers(repo):  # <--- Transformer receives one item at a time
        for page in client.paginate(f"repos/dlt-hub/{repo['name']}/stargazers"):
            yield page

    return (
        github_repos,
        github_events,
        github_stargazers,
        github_pulls_comments,
    )


# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="dlt_aurel",
    destination="duckdb",
    dataset_name="github",
)


# Run the pipeline using the defined resource
load_info = pipeline.run(github_source())
print(load_info)


# Query the loaded data from 'pokemon_api' table
tbl_events = pipeline.dataset(dataset_type="default").github_events.df()
tbl_stargazers = pipeline.dataset(dataset_type="default").github_stargazers.df()
tbl_pulls_comments = pipeline.dataset(dataset_type="default").github_pulls_comments.df()

print(f"events nb cols: {tbl_events.columns.size}")
print(f"stargazers nb cols: {tbl_stargazers.columns.size:}")
print(f"pulls_comments nb cols: {tbl_pulls_comments.columns.size:}")


# with pipeline.sql_client() as client:
#     res = client.execute_sql("select * from github_stargazers where id='17202864'")
#     print("stargazer #17202864 :", res)
