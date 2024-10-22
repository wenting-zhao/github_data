import requests
from bs4 import BeautifulSoup
import json

def get_git_blame(repo_url, file_path, branch):
    """
    Performs a Git blame for a file in a GitHub repository.
    
    Args:
        repo_url (str): The URL of the GitHub repository.
        file_path (str): The path to the file within the repository.
    
    Returns:
        dict: A dictionary containing the Git blame information for the file.
    """
    # Construct the GitHub blame URL
    blame_url = f"{repo_url}/blame/{branch}/{file_path}"
    
    # Make a request to the GitHub blame page
    response = requests.get(blame_url)
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find all the blame information blocks
    blame_block_data = soup.find_all('script', {'data-target': 'react-app.embeddedData', 'type': 'application/json'})
    
    # Extract the blame information
    blame_info = {}
    for block in blame_block_data:
        try:
            blame_dict = json.loads(block.string)['payload']['blame']
        except:
            # This happens when the file cant be found so it cant be blamed.
            continue

        commit_oid_to_info = {}
        for commit_id, commit_info in blame_dict['commits'].items():
            commit_oid_to_info[commit_info['oid']] = {"committerEmail": commit_info["committerEmail"], "commit_id": commit_id}

        for blame_range_num, blame_range in blame_dict['ranges'].items():
            commit_oid = blame_range['commitOid']
            committer_email = commit_oid_to_info[commit_oid]["committerEmail"]
            # commit_id = commit_oid_to_info[commit_oid]["commit_id"]
            # if commit_id not in blame_info:  blame_info[commit_id] = {}
            if committer_email not in blame_info: blame_info[committer_email] = []
            # if committer_email not in blame_info[commit_id]: blame_info[commit_id][committer_email] = []
            # blame_info[commit_id][committer_email].append((blame_range['start'], blame_range['end']))
            if len(blame_info[committer_email]) > 0 and blame_info[committer_email][-1][1] == blame_range['start'] - 1:
                blame_info[committer_email][-1][1] = blame_range['end']
            else:
                blame_info[committer_email].append([blame_range['start'], blame_range['end']])
            # blame_info[committer_email].extend(list(range(blame_range['start'], blame_range['end']+1))) #because list of lists is not liked
    
    return blame_info

# # Example usage
repo_url = "https://github.com/wenting-zhao/github_data"
file_path = "get_blame.py"
branch = "main"
blame_info = get_git_blame(repo_url, file_path, branch)

print(blame_info)

from datasets import load_dataset, Dataset
from datetime import datetime
import re
import os
import boto3
from smart_open import open

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
s3 = session.client("s3")

# def gen():
start_time = datetime.now()
blamed_data = []
# dataset streaming (will only download the data as needed)
ds = load_dataset("wentingzhao/stack-v2-cpp-2011-windows", split="train")
# ds = load_dataset("bigcode/the-stack-v2", "C++", streaming=True, split="train")
for sample in iter(ds): 
    repo_url = f"https://github.com/{sample['repo_name']}"
    branch_name = sample['branch_name']
    s3_url = f"s3://softwareheritage/content/{sample['blob_id']}"
    with open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
        sample["content"] = fin.read().decode(sample["src_encoding"])

    blame_info = get_git_blame(repo_url, sample["path"], branch=branch_name)
    if len(blame_info) == 0: 
        print("Found nothing for ", repo_url)
        continue

    authors = []
    author_lines = []
    for author, written_lines in blame_info.items():
        authors.append(author)
        author_lines.append(written_lines)
    sample["authors"] = authors  
    sample["author_lines"] = author_lines  
    # sample["blame_info"] = blame_info  
    # sample["blame_info"] = {user: len(lines) for user, lines in blame_info.items()}

    for key, value in sample.items():
        if isinstance(value, datetime):
            sample[key] = sample[key].strftime("%Y-%m-%d %H:%M:%S")
        
    blamed_data.append(sample)

    time_diff = (datetime.now() - start_time).total_seconds() / 60.
    total_est_time = time_diff * len(ds) / len(blamed_data)
    print(f"est time (mins): {total_est_time:.2f}")

    if len(blamed_data) % 50 == 0:
        with open("blamed_data-2011-windows.json", 'w') as wf:
            json.dump(blamed_data, wf, indent=4)
        blamed_ds = Dataset.from_list(blamed_data)
        blamed_ds.push_to_hub("celinelee/stack-v2-cpp-2011-windows-blamed")

blamed_ds = Dataset.from_list(blamed_data)
blamed_ds.push_to_hub("celinelee/stack-v2-cpp-2011-windows-blamed")
