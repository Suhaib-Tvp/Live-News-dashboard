import streamlit as st
from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
import time


NEWS_API_KEY = "pub_0b73cdbc05a94fcfb88347ff453aaf45"
API_URL = f"https://newsdata.io/api/1/news?apikey={NEWS_API_KEY}&language=en&category=top"
INDIAN_EXPRESS_URL = "https://indianexpress.com/latest-news/"
REFRESH_INTERVAL = 30  # seconds


spark = SparkSession.builder \
    .appName("Live News Dashboard Streamlit") \
    .getOrCreate()


def analyze_sentiment(text):
    try:
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0:
            label = "Positive"
        elif polarity < 0:
            label = "Negative"
        else:
            label = "Neutral"
        return polarity, label
    except:
        return 0.0, "Neutral"


def detect_topic(title):
    title_lower = title.lower()
    if any(word in title_lower for word in ["election", "minister", "government", "parliament", "policy"]):
        return "Politics"
    elif any(word in title_lower for word in ["covid", "virus", "health", "vaccine", "hospital"]):
        return "Health"
    elif any(word in title_lower for word in ["stock", "market", "economy", "business", "trade"]):
        return "Business"
    elif any(word in title_lower for word in ["match", "tournament", "football", "cricket", "sports"]):
        return "Sports"
    elif any(word in title_lower for word in ["movie", "film", "music", "celebrity", "entertainment"]):
        return "Entertainment"
    elif any(word in title_lower for word in ["tech", "technology", "ai", "gadget", "software", "app"]):
        return "Technology"
    else:
        return "General"

def fetch_newsdata_api():
    try:
        resp = requests.get(API_URL)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, dict) or "results" not in data:
            return []

        results = []
        for art in data.get("results", []):
            title = art.get("title", "")
            link = art.get("link", "")
            polarity, label = analyze_sentiment(title)
            topic = detect_topic(title)
            results.append({
                "Title": title,
                "Source": art.get("source_id", "Newsdata.io"),
                "Sentiment Score": polarity,
                "Sentiment Label": label,
                "Topic": topic,
                "Link": link
            })
        return results

    except requests.exceptions.RequestException:
        return []

def fetch_indian_express():
    try:
        resp = requests.get(INDIAN_EXPRESS_URL, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        articles = []
        for headline in soup.select("div.title a"):
            title = headline.get_text(strip=True)
            link = headline.get("href")
            if title and link:
                polarity, label = analyze_sentiment(title)
                topic = detect_topic(title)
                articles.append({
                    "Title": title,
                    "Source": "Indian Express",
                    "Sentiment Score": polarity,
                    "Sentiment Label": label,
                    "Topic": topic,
                    "Link": link
                })
        return articles

    except requests.exceptions.RequestException:
        return []


st.set_page_config(page_title="Live News Dashboard", layout="wide")
st.title("📰 Live News Dashboard")
st.caption(f"Updates every {REFRESH_INTERVAL} seconds. Data from Newsdata.io & Indian Express.")

# Auto refresh every REFRESH_INTERVAL seconds
st_autorefresh = st.experimental_rerun if hasattr(st, 'experimental_rerun') else None
st_autorefresh_interval = st.experimental_memo if hasattr(st, 'experimental_memo') else None
st_autorefresh = st_autorefresh_interval

# Fetch data
api_data = fetch_newsdata_api()
ie_data = fetch_indian_express()
combined_data = api_data + ie_data

if combined_data:
    df = spark.createDataFrame(combined_data).toPandas()
    st.dataframe(df, use_container_width=True)
else:
    st.warning("No news data available right now.")

# Auto refresh every 30 seconds
st_autorefresh = st.experimental_rerun
time.sleep(REFRESH_INTERVAL)
st.experimental_rerun()
