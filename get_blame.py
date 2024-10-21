from ghapi.all import GhApi

# Replace these with your GitHub token, owner, repo, and file path
token = 'YOUR_GITHUB_TOKEN'
owner = 'REPO_OWNER'
repo = 'REPO_NAME'
file_path = 'PATH/TO/FILE'
branch = 'main'  # Default branch or any specific branch you want to blame

# Initialize the GhApi client
api = GhApi(owner=owner, repo=repo, token=token)

# Get the file's commit history to identify blame info
def get_blame(file_path, branch='main'):
    # Get the latest commit sha for the given branch
    ref_data = api.repos.get_branch(branch=branch)
    latest_commit_sha = ref_data.commit.sha

    # Get blame information via the contents API
    blame_info = api.repos.get_blame(
        owner=owner,
        repo=repo,
        ref=latest_commit_sha,
        path=file_path
    )

    # Parsing and displaying blame information
    for commit in blame_info.get('blame', []):
        author = commit['author']['name']
        commit_sha = commit['commit']['sha']
        lines = commit['ranges'][0]['lines']

        print(f"Commit: {commit_sha}")
        print(f"Author: {author}")
        print(f"Lines: {lines[0]} to {lines[1]}\n")

# Run the blame function
get_blame(file_path, branch)

