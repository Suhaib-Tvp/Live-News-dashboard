import os
import time
import requests
import pandas as pd
import streamlit as st
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType

# --- Fix Java error for PySpark on Streamlit Cloud ---
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# --- Create Spark session ---
spark = SparkSession.builder \
    .appName("Live News Dashboard Streamlit") \
    .getOrCreate()

# --- Sentiment analysis function ---
def analyze_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

def get_sentiment_label(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

# --- Register UDFs ---
sentiment_udf = udf(analyze_sentiment, FloatType())
label_udf = udf(get_sentiment_label, StringType())

# --- Function to fetch news ---
def fetch_news():
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        "country": "us",
        "apiKey": "pub_0b73cdbc05a94fcfb88347ff453aaf45",
 # Store API key in Streamlit secrets
        "pageSize": 10
    }
    response = requests.get(url, params=params)
    data = response.json()
    articles = data.get("articles", [])

    # Prepare data for Spark
    df = pd.DataFrame([{
        "title": article["title"],
        "source": article["source"]["name"],
        "link": article["url"],
        "about": article.get("description", "N/A")
    } for article in articles])

    sdf = spark.createDataFrame(df)

    # Apply sentiment analysis
    sdf = sdf.withColumn("sentiment_score", sentiment_udf(sdf["title"]))
    sdf = sdf.withColumn("sentiment_label", label_udf(sdf["sentiment_score"]))

    # Convert back to Pandas for Streamlit display
    pdf = sdf.toPandas()

    # Reorder columns
    pdf = pdf[["title", "source", "sentiment_score", "sentiment_label", "link", "about"]]
    return pdf

# --- Streamlit UI ---
st.set_page_config(page_title="Live News Dashboard", layout="wide")
st.title("📰 Live News Dashboard")
st.caption("Auto-updates every 30 seconds with sentiment analysis")

# Auto-refresh every 30 seconds
while True:
    news_df = fetch_news()
    
    # Convert link to clickable
    news_df["link"] = news_df["link"].apply(lambda x: f"[Read more]({x})")

    st.dataframe(news_df, use_container_width=True)

    time.sleep(30)
    st.experimental_rerun()
