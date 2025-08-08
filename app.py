import os
import time
import requests
import pandas as pd
import streamlit as st
from textblob import TextBlob
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType

# -------------------- CONFIGURE JAVA FOR STREAMLIT CLOUD --------------------
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# -------------------- STREAMLIT PAGE SETTINGS --------------------
st.set_page_config(page_title="📰 Live News Dashboard", layout="wide")
st.title("📰 Live News Dashboard")
st.caption("Auto-updates every 30 seconds with sentiment analysis and topic detection")

# -------------------- API KEY HANDLING --------------------
try:
    NEWS_API_KEY = st.secrets["NEWS_API_KEY"]
except KeyError:
    NEWS_API_KEY = "pub_0b73cdbc05a94fcfb88347ff453aaf45"  # fallback for local dev

# -------------------- START SPARK SESSION --------------------
spark = SparkSession.builder.appName("LiveNewsDashboard").getOrCreate()

# -------------------- UDFS FOR SENTIMENT --------------------
def get_sentiment_score(text):
    if not text:
        return 0.0
    return TextBlob(text).sentiment.polarity

def get_sentiment_label(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(get_sentiment_score, FloatType())
label_udf = udf(get_sentiment_label, StringType())

# -------------------- TOPIC DETECTION --------------------
def detect_topic(text):
    if not text:
        return "Other"
    text = text.lower()
    if any(word in text for word in ["politics", "government", "election"]):
        return "Politics"
    elif any(word in text for word in ["health", "covid", "vaccine", "disease"]):
        return "Health"
    elif any(word in text for word in ["business", "market", "economy", "stock"]):
        return "Business"
    elif any(word in text for word in ["sports", "cricket", "football", "tournament"]):
        return "Sports"
    else:
        return "Other"

topic_udf = udf(detect_topic, StringType())

# -------------------- FETCH FROM NEWSDATA.IO --------------------
def fetch_newsdata_api():
    url = "https://newsdata.io/api/1/news"
    params = {
        "apikey": NEWS_API_KEY,
        "language": "en",
        "category": "top"
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            st.warning(f"Newsdata.io API Error: {response.status_code}")
            return pd.DataFrame(columns=["title", "source", "link", "about"])
        
        articles = response.json().get("results", [])
        if not articles:
            return pd.DataFrame(columns=["title", "source", "link", "about"])
        
        return pd.DataFrame([{
            "title": art.get("title", ""),
            "source": art.get("source_id", "Unknown"),
            "link": art.get("link", ""),
            "about": art.get("description", "")
        } for art in articles])
    except Exception as e:
        st.warning(f"Error fetching Newsdata.io: {e}")
        return pd.DataFrame(columns=["title", "source", "link", "about"])

# -------------------- SCRAPE THE INDIAN EXPRESS --------------------
def scrape_indian_express():
    url = "https://indianexpress.com/"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            st.warning("Indian Express scraping failed")
            return pd.DataFrame(columns=["title", "source", "link", "about"])
        
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = soup.find_all("h2", class_="title")
        if not headlines:
            return pd.DataFrame(columns=["title", "source", "link", "about"])
        
        data = []
        for h in headlines:
            a_tag = h.find("a")
            if a_tag:
                title = a_tag.text.strip()
                link = a_tag["href"]
                data.append({
                    "title": title,
                    "source": "The Indian Express",
                    "link": link,
                    "about": ""
                })
        return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Error scraping Indian Express: {e}")
        return pd.DataFrame(columns=["title", "source", "link", "about"])

# -------------------- FETCH + PROCESS ALL NEWS --------------------
def fetch_news():
    df_api = fetch_newsdata_api()
    df_scrape = scrape_indian_express()

    df = pd.concat([df_api, df_scrape], ignore_index=True).drop_duplicates(subset=["title"])
    if df.empty:
        return pd.DataFrame(columns=["title", "source", "sentiment_score", "sentiment_label", "topic", "link", "about"])

    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(df)

    # Sentiment + Topic Detection
    sdf = sdf.withColumn("sentiment_score", sentiment_udf(sdf["title"]))
    sdf = sdf.withColumn("sentiment_label", label_udf(sdf["sentiment_score"]))
    sdf = sdf.withColumn("topic", topic_udf(sdf["title"]))

    # Convert back to Pandas
    pdf = sdf.toPandas()
    return pdf[["title", "source", "sentiment_score", "sentiment_label", "topic", "link", "about"]]

# -------------------- STREAMLIT LIVE UPDATE --------------------
placeholder = st.empty()

while True:
    news_df = fetch_news()
    if news_df.empty:
        placeholder.warning("No news available right now.")
    else:
        placeholder.dataframe(news_df, use_container_width=True)
    time.sleep(30)
