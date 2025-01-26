"""The Github API templates provides a starting point to read data from REST APIs with REST Client helper"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional

import dlt

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client import RESTClient
# from dlt.sources.helpers.rest_client import paginate
# from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.source
def github_source(access_token: Optional[str] = dlt.secrets.value):
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(access_token) if access_token else None,
    )

    @dlt.resource
    def github_events():
        for page in client.paginate("orgs/dlt-hub/events"):
            yield page

    @dlt.resource
    def github_stargazers():
        for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
            yield page

    return github_events, github_stargazers  # , github_issues


# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="dlt_demo",
    destination="duckdb",
    dataset_name="github_dlt_paginated",
)


# Run the pipeline using the defined resource
load_info = pipeline.run(github_source())
print(load_info)


# Query the loaded data from 'pokemon_api' table
tbl_events = pipeline.dataset(dataset_type="default").github_events.df()
tbl_stargazers = pipeline.dataset(dataset_type="default").github_stargazers.df()

print(f"events nb cols: {tbl_events.columns.size}")
print(f"stargazers nb cols: {tbl_stargazers.columns.size:}")


with pipeline.sql_client() as client:
    res = client.execute_sql("select * from github_stargazers where id='17202864'")
    print("stargazer #17202864 :", res)
