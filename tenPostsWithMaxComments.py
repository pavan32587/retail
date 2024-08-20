from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import requests

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TopTenPosts") \
    .getOrCreate()

# Function to load data from API into Spark DataFrame
def load_api_data_to_spark(api_url):
    response = requests.get(api_url)
    data = response.json()
    return spark.read.json(spark.sparkContext.parallelize(data))

# Load data directly into Spark DataFrames
posts_sdf = load_api_data_to_spark("https://jsonplaceholder.typicode.com/posts")
comments_sdf = load_api_data_to_spark("https://jsonplaceholder.typicode.com/comments")
users_sdf = load_api_data_to_spark("https://jsonplaceholder.typicode.com/users")

# Transformation: Group comments by postId and count the number of comments per post
posts_comments_count = comments_sdf.groupBy("postId").agg(count("*").alias("commentsNumber"))

# Transformation: Join the posts data with the comments count
posts_with_comments = posts_sdf.join(posts_comments_count, posts_sdf.id == posts_comments_count.postId, "inner")

# Transformation: Join the result with users data
result = posts_with_comments.join(users_sdf, posts_with_comments.userId == users_sdf.id, "inner")

# Transformation: Select and rename the necessary columns
final_result = result.select(
    col("postId"),
    col("commentsNumber"),
    col("userId"),
    col("username").alias("userName")
)

# Transformation: Order by number of comments in descending order and limit to top 10
top_ten_posts = final_result.orderBy(desc("commentsNumber")).limit(10)

# Action: Show the result on console (can be used for debugging)
top_ten_posts.show()

# Action: Save the final DataFrame to a Parquet file
output_path = "tenPostsWithMaxComments.parquet"
top_ten_posts.write.parquet(output_path)

# Action: Collect the data into a list of rows (if you want to process it in the driver)
result_rows = top_ten_posts.collect()

# Stop the Spark Session
spark.stop()

# Print the collected results
for row in result_rows:
    print(row)
