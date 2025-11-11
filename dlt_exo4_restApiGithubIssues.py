import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": dlt.secrets["sources.access_token"],
        },
        "paginator": "header_link",  # <---- set up paginator type
    },
    "resources": [  # <--- list resources
        {
            "name": "issues",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues",
                "params": {
                    "state": "open",
                },
            },
        },
        {
            "name": "contributors",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/contributors",
            },
        },
        {
            "name": "issue_comments",  # <-- here we declare dlt.transformer
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                "params": {
                    "issue_number": {
                        "type": "resolve",  # <--- use type 'resolve' to resolve {issue_number} for transformer
                        "resource": "issues",
                        "field": "number",
                    },
                },
            },
        },
    ],
}

github_source = rest_api_source(config)


pipeline = dlt.pipeline(
    pipeline_name="dlt_demo",
    destination="duckdb",
    dataset_name="github_issues_exo4_1",
    dev_mode=True,
)

load_info = pipeline.run(github_source)
print(load_info)

issues = pipeline.dataset(dataset_type="default").issues.df()
contributors = pipeline.dataset(dataset_type="default").contributors.df()

print(f"issues nb cols: {issues.columns.size}")
print(f"contributors nb cols: {contributors.columns.size}")
