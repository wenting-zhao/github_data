
### Collect data from the stack v2 (https://huggingface.co/datasets/bigcode/the-stack-v2)
##### cpp
##### < 2010
##### go by function chunks
##### map to author using git blame


from datasets import load_dataset
import re
from datetime import datetime
from get_blame import get_blame
import os
import boto3
from smart_open import open


session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
s3 = session.client("s3")

# dataset streaming (will only download the data as needed)
ds = load_dataset("bigcode/the-stack-v2", streaming=True, split="train")
for sample in iter(ds): 
    if sample['lang'] not in {"c++"}: continue

    year = re.match(r'(20\d\d)-.+', sample["revision_date"])
    if year > 2011: continue

    parsed_repo_name = re.search(r'([^/]+/(.+))', sample["repo_name"])
    repo_owner = parsed_repo_name.group(1)
    repo_name = parsed_repo_name.group(2)
    branch_name = sample['branch_name']

    file_paths = []
    for file in sample["files"]:
        if file["language"] not in {"c++"}: continue
        s3_url = f"s3://softwareheritage/content/{file['blob_id']}"
        with open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
            file["content"] = fin.read().decode(file["src_encoding"])
        file_paths.append(file["path"])

    blame_data = get_blame(repo_owner, repo_name, file_paths, branch=branch_name)
    import pdb; pdb.set_trace()
