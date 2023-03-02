import datetime
from typing import Generator

import requests
import requests_cache
import urllib3
import yaml
from github import Github

requests_cache.install_cache('github_cache')


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




# Set the access token and header
headers = {"Authorization": "Bearer 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"}

# Set the GraphQL query
base_query = """
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
"""
paginated_query = """
query {
  search(query: "language:python stars:>200", type: REPOSITORY, first: 100, after:{AFTER}) {
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
"""
retry = RateLimitRetry(status_forcelist=[403])
gh = Github(
    login_or_token="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",
    retry=retry
)
# Send the GraphQL request
response = requests.post('https://api.github.com/graphql', headers=headers, json={'query': base_query})

# Check if the request was successful
if response.status_code != 200:
    raise Exception(f"Request failed with status code {response.status_code}. Error message: {response.text}")

# Process the response data
data = response.json()['data']['search']['nodes']

# Search for "pytest" in the code of each workflow file in the repositories
for repo in data:
    url = repo['url']
    repo_name = repo['nameWithOwner']
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
    pass


if __name__ == '__main__':
    main()
