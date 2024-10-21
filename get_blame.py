import requests
from bs4 import BeautifulSoup

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
    blame_blocks = soup.find_all("div", class_="blame-hunk")
    
    # Extract the blame information
    blame_info = {}
    for block in blame_blocks:
        commit_hash = block.find("a", class_="js-navigation-open")["href"].split("/")[-1]
        commit_author = block.find("span", class_="blame-commit-author").text.strip()
        commit_timestamp = block.find("tag-timestamp")["datetime"]
        blame_info[commit_hash] = {
            "author": commit_author,
            "timestamp": commit_timestamp
        }
    
    return blame_info

# Example usage
repo_url = "https://github.com/wenting-zhao/github_data"
file_path = "get_blame.py"
branch = "main"
blame_info = get_git_blame(repo_url, file_path, branch)
