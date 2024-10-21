from ghapi.all import GhApi
import os

# Get environment variables
token = os.getenv('GITHUB_TOKEN')

# Get the file's commit history to identify blame info
def get_blame(repo_owner, repo_name, file_paths, branch='main'):
    import pdb; pdb.set_trace()
    # Initialize the GhApi client
    api = GhApi(owner=repo_owner, repo=repo_name, token=token)
    # Get the latest commit sha for the given branch
    ref_data = api.repos.get_branch(branch=branch)
    latest_commit_sha = ref_data.commit.sha
    
    blame_data = {}

    for file_path in file_paths:
        # Get blame information via the contents API
        blame_info = api.repos.get_blame(
            owner=repo_owner,
            repo=repo_name,
            ref=latest_commit_sha,
            path=file_path
        )
        blame_data[file_path] = []

        # Parsing and displaying blame information
        for commit in blame_info.get('blame', []):
            author = commit['author']['name']
            commit_sha = commit['commit']['sha']
            lines = commit['ranges'][0]['lines']

            blame_data[file_path].append({
                "commmit": commit_sha,
                "author": author,
                "lines": lines
            })
    return blame_data

