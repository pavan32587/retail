import pandas as pd  # data manipulation and analysis
import requests  # making HTTP requests to fetch data from APIs
import json  # handling JSON data 
import os  # operating system dependent functionalities like file paths
import logging  # logging information, errors, and debugging
import pyarrow as pa  # handling Parquet files

logging.basicConfig(level=logging.INFO)

def load_api_data_to_pandas(api_url):
    logging.info(f"Fetching data from {api_url}")
    response = requests.get(api_url)
    data = response.json()
    return pd.DataFrame(data)

posts_df = load_api_data_to_pandas("https://jsonplaceholder.typicode.com/posts")
comments_df = load_api_data_to_pandas("https://jsonplaceholder.typicode.com/comments")
users_df = load_api_data_to_pandas("https://jsonplaceholder.typicode.com/users")

posts_comments_count = comments_df.groupby("postId").size().reset_index(name="commentsNumber")
posts_with_comments = pd.merge(posts_df, posts_comments_count, left_on="id", right_on="postId", how="inner")
result = pd.merge(posts_with_comments, users_df, left_on="userId", right_on="id", how="inner")
final_result = result[["postId", "commentsNumber", "userId", "username"]].rename(columns={"username": "userName"})
top_ten_posts = final_result.sort_values(by="commentsNumber", ascending=False).head(10)
output_path = "tenPostsWithMaxComments.parquet"
top_ten_posts.to_parquet(output_path, index=False)
print(top_ten_posts)
