
### Collect data from the stack v2 (https://huggingface.co/datasets/bigcode/the-stack-v2)
##### cpp
##### < 2010
##### go by function chunks
##### map to author using git blame

from datasets import load_dataset

ds = load_dataset("bigcode/the-stack-v2", "C++", split="train")
ds = ds.filter(lambda example: example["revision_date"].year <= 2019, num_proc=8)
ds.push_to_hub("celinelee/stack-v2-cpp-2019")

