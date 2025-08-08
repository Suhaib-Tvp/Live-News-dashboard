import streamlit as st
import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
import pandas as pd
from pyspark.sql import SparkSession

# Initialize PySpark
spark = SparkSession.builder.appName("LiveNewsDashboard").getOrCreate()

st.set_page_config(page_title="📰 Live News Dashboard", layout="wide")
st.title("📰 Live News Dashboard")
st.caption("Auto-updates every 30 seconds with sentiment analysis")

# Built-in autorefresh (no external package)
st.experimental_rerun  # available for manual reruns
st_autorefresh = st.experimental_rerun  # alias if needed

# Auto-refresh every 30 seconds without while-loop
if st.session_state.get("last_refresh", 0) + 30 < pd.Timestamp.now().timestamp():
    st.session_state.last_refresh = pd.Timestamp.now().timestamp()
    st.experimental_rerun()

# Sentiment analysis
def analyze_sentiment(text):
    if not text or not isinstance(text, str):
        return 0.0, "Neutral"
    score = TextBlob(text).sentiment.polarity
    if score > 0:
        label = "Positive"
    elif score < 0:
        label = "Negative"
    else:
        label = "Neutral"
    return score, label

# Topic detection
def detect_topic(text):
    if not text:
        return "Unknown"
    text_lower = text.lower()
    if "politic" in text_lower:
        return "Politics"
    elif "health" in text_lower:
        return "Health"
    elif "business" in text_lower or "market" in text_lower:
        return "Business"
    elif "sport" in text_lower:
        return "Sports"
    elif "tech" in text_lower:
        return "Technology"
    else:
        return "General"

# Newsdata.io API
def fetch_newsdata_io():
    try:
        url = "https://newsdata.io/api/1/news"
        params = {
            "apikey": st.secrets.get("NEWS_API_KEY", ""),
            "language": "en",
            "category": "top"
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        articles = resp.json().get("results", [])
        news_list = []
        for a in articles:
            score, label = analyze_sentiment(a.get("title"))
            topic = detect_topic(a.get("title"))
            news_list.append({
                "title": a.get("title"),
                "source": a.get("source_id"),
                "sentiment_score": score,
                "sentiment_label": label,
                "topic": topic,
                "link": a.get("link")
            })
        return news_list
    except Exception as e:
        st.error(f"Error fetching Newsdata.io: {e}")
        return []

# Indian Express scraper
def scrape_indian_express():
    try:
        url = "https://indianexpress.com/"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        headlines = soup.select("h2.title")
        news_list = []
        for h in headlines:
            title = h.get_text(strip=True)
            link_tag = h.find("a")
            link = link_tag["href"] if link_tag else None
            score, label = analyze_sentiment(title)
            topic = detect_topic(title)
            news_list.append({
                "title": title,
                "source": "Indian Express",
                "sentiment_score": score,
                "sentiment_label": label,
                "topic": topic,
                "link": link
            })
        return news_list
    except Exception as e:
        st.error(f"Error scraping Indian Express: {e}")
        return []

# Fetch and display news
def fetch_news():
    all_news = fetch_newsdata_io() + scrape_indian_express()
    if not all_news:
        return pd.DataFrame(columns=["title", "source", "sentiment_score", "sentiment_label", "topic", "link"])
    return pd.DataFrame(all_news)

news_df = fetch_news()

if not news_df.empty:
    sdf = spark.createDataFrame(news_df)
    st.dataframe(news_df[["title", "source", "sentiment_score", "sentiment_label", "topic", "link"]])
else:
    st.warning("No news available at the moment.")
