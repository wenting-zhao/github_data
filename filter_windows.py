from datasets import load_dataset
import datetime
import re
import os
import boto3
from smart_open import open
from tqdm import tqdm

def filter_sample(sample):
    try:
        s3_url = f"s3://softwareheritage/content/{sample['blob_id']}"
        with open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
            content = fin.read().decode(sample["src_encoding"])
        return "\r\n" in content
    except Exception as e:
        print(e)
        return False

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
s3 = session.client("s3")

ds = load_dataset("wentingzhao/stack-v2-cpp-2011", split="train").filter(filter_sample, num_proc=8)
ds.push_to_hub("wentingzhao/stack-v2-cpp-2011-windows")
