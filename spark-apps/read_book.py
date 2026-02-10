"""
Word Count and Text Analysis Script

This script performs text analysis on a book (text file) using Apache Spark.

Problem Description:
    The primary goal is to count the number of occurrences of each word in a 
    text file. This allows for statistical analysis of word frequency and text patterns.

Functionality:
    - Reads text data from a file
    - Processes and cleans words (lowercasing, extracting alphabetic characters)
    - Counts word occurrences across the entire document
    - Displays the top 10 most frequent words
    - Exports word count results to CSV format

Data Source:
    Default: /data/1342-0.txt (Project Gutenberg text files)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, regexp_extract


spark = SparkSession.builder.appName("ReadBook").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

book = spark.read.text("/data/1342-0.txt")

# Split each line of text into an array of words using space as delimiter
lines = book.select(split(book.value, " ").alias("line"))

# Flatten the arrays into individual rows, one word per row
words = lines.select(explode(col("line")).alias("word"))

# Convert all words to lowercase for case-insensitive processing
words_lower = words.select(lower(col("word")).alias("word_lower"))

# Extract only alphabetic characters (a-z), removing punctuation and numbers
words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word"))

# Filter out empty strings that result from non-alphabetic entries
words_nonull = words_clean.filter(col("word") != "")

# Group by each word and count occurrences
results = words_nonull.groupby(col("word")).count()

# Display the top 10 most frequent words, sorted by count in descending order
results.orderBy("count", ascending=False).show(10)
# Display the top 10 most frequent words without truncating the output
results.orderBy(col("count").desc()).show(10, truncate=False)

results.write.csv("/data/word_counts.csv")
