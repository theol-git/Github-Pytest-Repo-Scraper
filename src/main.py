import datetime
from textwrap import dedent
from typing import Generator

import requests
import requests_cache
import urllib3
import yaml
from github import Github

requests_cache.install_cache('github_cache')
GITHUB_API_KEY = "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"


class RateLimitRetry(urllib3.util.retry.Retry):
    def get_retry_after(self, response):
        reset_time = datetime.datetime.fromtimestamp(int(response.headers["X-RateLimit-Reset"]))
        retry_after = min((reset_time - datetime.datetime.now()).seconds + 5, 60)

        print(f"Rate limited,  retry after: {retry_after} seconds")
        return retry_after


def find_by_key(data, target):
    for key, value in data.items():
        if isinstance(value, dict):
            yield from find_by_key(value, target)
        elif key == target:
            yield value
        elif isinstance(value, list):
            for items in value:
                if isinstance(items, dict):
                    yield from find_by_key(items, target)


def get_python_repositories() -> Generator[dict, None, None]:
    session = requests.Session()
    # Set the access token and header
    session.headers = {"Authorization": f"Bearer {GITHUB_API_KEY}"}
    graphql_endpoint = 'https://api.github.com/graphql'
    # Set the GraphQL query
    base_query = dedent("""\
    query {
      search(query: "language:python stars:>200", type: REPOSITORY, first: 100) {
        nodes {
          ... on Repository {
            nameWithOwner
            url
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """)
    # Send the initial GraphQL request
    response = session.post(graphql_endpoint, json={'query': base_query})

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}. Error message: {response.text}")

    # Process the response data
    response_data = response.json()
    data = response_data['data']['search']['nodes']
    yield from data

    page_info = response_data['data']['search']['pageInfo']
    has_next_page = page_info['hasNextPage']
    cursor = page_info['endCursor']

    paginated_query = dedent("""\
        query {
          search(query: "language:python stars:>200", type: REPOSITORY, first: 100, after:"{AFTER}") {
            nodes {
              ... on Repository {
                nameWithOwner
                url
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """)

    while has_next_page:
        query = paginated_query.format(AFTER=cursor)

        # Send the initial GraphQL request
        response = session.post(graphql_endpoint, json={'query': query})

        # Check if the request was successful
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}. Error message: {response.text}")

        # Process the response data
        response_data = response.json()
        data = response_data['data']['search']['nodes']
        yield from data

        page_info = response_data['data']['search']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        # lets ignore the cache on this for the moment and rely on requests_cache, this is not the bottleneck whatsoever


def parse_repo(gh: Github, url: str, repo_name: str):
    # Send a search request for ".yml" files containing "pytest" in the ".github/workflows" directory of the repository
    search_query = f"pytest in:file filename:*.yml path:.github/workflows repo:{repo_name}"

    search_results = gh.search_code(
        query=search_query,
    )
    found_valid_pytest = False
    for file in search_results:
        # Retrieve the contents of the file
        file_content = file.decoded_content.decode()
        # Load the file as YAML
        file_yaml = yaml.safe_load(file_content)

        # Check if the YAML file contains a job that runs pytest
        pytest_handled = False
        for run in find_by_key(file_yaml, "run"):
            run_bk = run  # TODO: remove
            run: str = run.replace("\\\n", '')
            for line in run.split('\n'):
                if line.startswith("pip install") or line.startswith('python -m pip install'):
                    continue
                if 'pytest' in line:
                    print()
                    pytest_handled = True
                if line in open('../res/valid.txt').read():
                    pytest_handled = True

        if not pytest_handled:
            print()


# todo save in database by projectname-filename-linenumber, skip if already seen

def check_if_pytest(line: str) -> bool:
    words = line.split()
    if words[0] == "pytest":
        return True
    if words[0:3] == ["python", "-m", "pytest"]:
        return True
    if words[0:2] == ["docker", "run"] and words[3] == "pytest":
        return True
    if words[0:3] == ["poetry", "run", "pytest"]:
        return True
    return False


def main():
    retry = RateLimitRetry(status_forcelist=[403])
    gh = Github(
        login_or_token=GITHUB_API_KEY,
        retry=retry
    )
    # Search for "pytest" in the code of each workflow file in the repositories
    for repo in get_python_repositories():
        url = repo['url']
        repo_name = repo['nameWithOwner']
        parse_repo(gh, url, repo_name)


if __name__ == '__main__':
    main()
