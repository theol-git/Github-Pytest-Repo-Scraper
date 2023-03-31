import base64
import datetime
import logging
from enum import Enum
from textwrap import dedent
from typing import Generator

import requests
import requests_cache
import urllib3
import yaml
from github import Github
from redis import Redis

requests_cache.install_cache('/database/github_cache', stale_if_error=True)
GITHUB_API_KEY = "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"

file_status_db = Redis(host='redis', port=6379, db=0)
valid_db = Redis(host='redis', port=6379, db=1)
unsure_db = Redis(host='redis', port=6379, db=2)
invalid_db = Redis(host='redis', port=6379, db=3)
no_pytest_repos_db = Redis(host='redis', port=6379, db=4)
pytest_not_in_run_files_db = Redis(host='redis', port=6379, db=5)


class RateLimitRetry(urllib3.util.retry.Retry):
    logger = logging.getLogger("Rate Limiter")

    def get_retry_after(self, response):
        reset_time = datetime.datetime.fromtimestamp(int(response.headers["X-RateLimit-Reset"]))
        retry_after = min((reset_time - datetime.datetime.now()).seconds + 5, 60)
        self.logger.warning(f"retry after: {retry_after} seconds")
        return retry_after


def find_by_key(data: dict, searched_key: str):
    logger = logging.getLogger("Key search")

    found_value = False
    for key, value in data.items():
        if isinstance(value, dict):
            yield from find_by_key(value, searched_key)
        elif key == searched_key:
            found_value = True
            yield value
        elif isinstance(value, list):
            for items in value:
                if isinstance(items, dict):
                    yield from find_by_key(items, searched_key)
    if not found_value:
        logger.warning(f"Could not find any occurrence of the key '{searched_key}' in {data}")


def get_python_repositories() -> Generator[dict, None, None]:
    logger = logging.getLogger("Graphql Requester")

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

    session = requests.Session()
    # Set the access token and header
    session.headers = {"Authorization": f"Bearer {GITHUB_API_KEY}"}
    graphql_endpoint = 'https://api.github.com/graphql'

    while get_python_repositories.has_next_page:
        if get_python_repositories.cursor is None:
            logger.info('Getting initial page')
            query = base_query
        else:
            logger.info("Getting next page")
            query = paginated_query.format(AFTER=get_python_repositories.cursor)

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
        get_python_repositories.has_next_page = page_info['hasNextPage']
        get_python_repositories.cursor = page_info['endCursor']

        logger.info(
            f'Got {len(data)} projects - '
            f'has_next_page: {get_python_repositories.has_next_page} - '
            f'cursor: {base64.b64decode(get_python_repositories.cursor)}'
        )


# TODO: backup to redis in order to continue where we left off
# lets ignore for the moment and rely on requests_cache, not the bottleneck whatsoever
get_python_repositories.cursor = None
get_python_repositories.has_next_page = True


def parse_repo(gh: Github, url: str, repo_name: str):
    logger = logging.getLogger("Repo Parser")

    if file_status_db.get(repo_name) == 1:
        logger.info(f"Skipped {repo_name}")
        return

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
            run: str = run.replace("\\\n", '')
            for line_no, line in enumerate(run.strip().split('\n')):
                match check_if_pytest(line):
                    case PytestStatus.VALID:
                        valid_db.incr(line)
                        logger.info(f"Found valid line: {line}")
                        pytest_handled = True
                        found_valid_pytest = True
                    case PytestStatus.INVALID:
                        logger.info(f"Found invalid line: {line}")
                        pytest_handled = True
                        invalid_db.incr(line)
                    case PytestStatus.UNSURE:
                        logger.info(f"Found unsure line: {line}")
                        pytest_handled = True
                        unsure_db.incr(line)

        if not pytest_handled:
            pytest_not_in_run_files_db.set(f"{repo_name}-{file.name}", 1)

    if not found_valid_pytest:
        no_pytest_repos_db.set(repo_name, 1)
    file_status_db.set(repo_name, 1)


class PytestStatus(Enum):
    VALID = 1
    INVALID = 2
    UNSURE = 3


def check_if_pytest(line: str) -> PytestStatus:
    words = line.split()
    if words[0] == "pytest" or \
            words[0:3] == ["python", "-m", "pytest"] or \
            words[0:2] == ["docker", "run"] and words[3] == "pytest" or \
            words[0:3] == ["poetry", "run", "pytest"]:
        return PytestStatus.VALID
    if words[0:2] == ["pip", "install"] or \
            words[0:4] == ["python", "-m", "pip", "install"]:
        return PytestStatus.INVALID
    return PytestStatus.UNSURE


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
