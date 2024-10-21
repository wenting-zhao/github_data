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
            import pdb; pdb.set_trace()
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
    
    return blame_info

# # Example usage
repo_url = "https://github.com/wenting-zhao/github_data"
file_path = "get_blame.py"
branch = "main"
blame_info = get_git_blame(repo_url, file_path, branch)

print(blame_info)


from datasets import load_dataset
import datetime
import re
import os
import boto3
from smart_open import open

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
s3 = session.client("s3")

blamed_data = []
# dataset streaming (will only download the data as needed)
# ds = load_dataset("wentingzhao/stack-v2-cpp-2011", streaming=True, split="train")
ds = load_dataset("bigcode/the-stack-v2", "C++", streaming=True, split="train")
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
    sample["blame_info"] = blame_info
    for key, value in sample.items():
        if isinstance(value, datetime.datetime):
            sample[key] = sample[key].strftime("%Y-%m-%d %H:%M:%S")
        
    blamed_data.append(sample)

    with open("blamed_data.json", 'w') as wf:
        json.dump(blamed_data, wf, indent=4)
    
ds = Dataset.from_list(blamed_data)
ds.push_to_hub("celinelee/stack-v2-cpp-2011-blamed")
