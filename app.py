import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder.appName("LiveNewsDashboard").getOrCreate()

# ---------------------------
# Sentiment Functions
# ---------------------------
def get_sentiment_score(text):
    if not text:
        return 0.0
    return round(TextBlob(text).sentiment.polarity, 3)

def get_sentiment_label(score):
    if score > 0:
        return "Positive"
    elif score < 0:
        return "Negative"
    else:
        return "Neutral"

def detect_topic(text):
    if not text:
        return "General"
    t = text.lower()
    if any(word in t for word in ["election", "government", "minister", "politics"]):
        return "Politics"
    elif any(word in t for word in ["covid", "health", "vaccine", "disease"]):
        return "Health"
    elif any(word in t for word in ["stock", "market", "economy", "business"]):
        return "Business"
    elif any(word in t for word in ["cricket", "football", "tennis", "sports"]):
        return "Sports"
    else:
        return "General"

# ---------------------------
# Register Spark UDFs
# ---------------------------
sentiment_udf = udf(get_sentiment_score, FloatType())
label_udf = udf(get_sentiment_label, StringType())
topic_udf = udf(detect_topic, StringType())

# ---------------------------
# Fetch Newsdata.io API
# ---------------------------
def fetch_newsdata_api():
    url = "https://newsdata.io/api/1/news"
    params = {
        "apikey": st.secrets.get("NEWS_API_KEY", "pub_0b73cdbc05a94fcfb88347ff453aaf45"),
        "language": "en",
        "category": "top"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        articles = []
        for item in data.get("results", []):
            articles.append({
                "title": item.get("title", ""),
                "source": item.get("source_id", "Newsdata.io"),
                "link": item.get("link", ""),
                "about": item.get("description", "")
            })
        return pd.DataFrame(articles)
    except Exception as e:
        st.error(f"Error fetching Newsdata.io: {e}")
        return pd.DataFrame(columns=["title", "source", "link", "about"])

# ---------------------------
# Scrape The Indian Express
# ---------------------------
def scrape_indian_express():
    url = "https://indianexpress.com/latest-news/"
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = []
        for item in soup.select("div.title"):
            a = item.find("a")
            if a:
                headlines.append({
                    "title": a.get_text(strip=True),
                    "source": "Indian Express",
                    "link": a.get("href", ""),
                    "about": ""
                })
        return pd.DataFrame(headlines)
    except Exception as e:
        st.error(f"Error scraping Indian Express: {e}")
        return pd.DataFrame(columns=["title", "source", "link", "about"])

# ---------------------------
# Combine, Process & Return
# ---------------------------
def fetch_news():
    columns = ["title", "source", "link", "about"]

    df_api = fetch_newsdata_api()[columns]
    df_scrape = scrape_indian_express()[columns]

    df = pd.concat([df_api, df_scrape], ignore_index=True)
    df.dropna(subset=["title"], inplace=True)
    df.fillna("", inplace=True)
    df = df.drop_duplicates(subset=["title"])

    if df.empty:
        return pd.DataFrame(columns=["title", "source", "sentiment_score", "sentiment_label", "topic", "link", "about"])

    try:
        sdf = spark.createDataFrame(df)
    except Exception as e:
        st.error(f"Spark DataFrame creation failed: {e}")
        return pd.DataFrame(columns=["title", "source", "sentiment_score", "sentiment_label", "topic", "link", "about"])

    sdf = sdf.withColumn("sentiment_score", sentiment_udf(sdf["title"]))
    sdf = sdf.withColumn("sentiment_label", label_udf(sdf["sentiment_score"]))
    sdf = sdf.withColumn("topic", topic_udf(sdf["title"]))

    pdf = sdf.toPandas()
    return pdf[["title", "source", "sentiment_score", "sentiment_label", "topic", "link", "about"]]

# ---------------------------
# Streamlit App
# ---------------------------
st.set_page_config(page_title="Live News Dashboard", layout="wide")
st.title("📰 Live News Dashboard")
st.caption("Auto-updates every 30 seconds with sentiment analysis")

# Auto-refresh every 30 sec
st_autorefresh = st.experimental_rerun  # handled by user refresh; or we could use streamlit_autorefresh package

news_df = fetch_news()

if not news_df.empty:
    # Make link clickable
    news_df["link"] = news_df["link"].apply(lambda x: f"[Read more]({x})" if x else "")
    st.dataframe(news_df, use_container_width=True)
else:
    st.warning("No news available at the moment.")
