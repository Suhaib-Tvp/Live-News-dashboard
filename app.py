import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from textblob import TextBlob

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="📰 Live News Dashboard", layout="wide")
API_KEY = st.secrets.get("NEWS_API_KEY", "")  # safely load from secrets

# ----------------------------
# FUNCTIONS
# ----------------------------

def fetch_api_news():
    """Fetch news from Newsdata.io API."""
    if not API_KEY:
        st.warning("⚠️ No API key found in Streamlit Secrets. Only scraped news will be shown.")
        return []

    url = "https://newsdata.io/api/1/news"
    params = {"apikey": API_KEY, "language": "en"}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        articles = data.get("results", [])
        return [{"title": a.get("title", ""),
                 "link": a.get("link", ""),
                 "source": "Newsdata.io"} for a in articles]
    except Exception as e:
        st.error(f"API error: {e}")
        return []

def fetch_scraped_news():
    """Scrape headlines from The Indian Express."""
    url = "https://indianexpress.com/latest-news/"
    try:
        r = requests.get(url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        headlines = soup.select("div.title")
        return [{"title": h.get_text(strip=True),
                 "link": h.find("a")["href"] if h.find("a") else "",
                 "source": "Indian Express"} for h in headlines]
    except Exception as e:
        st.error(f"Scraping error: {e}")
        return []

def analyze_sentiment(text):
    """Return sentiment polarity and label."""
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return polarity, "Positive"
    elif polarity < 0:
        return polarity, "Negative"
    return polarity, "Neutral"

def detect_topic(title):
    """Simple keyword-based topic detection."""
    title_lower = title.lower()
    if any(word in title_lower for word in ["election", "government", "minister", "politics"]):
        return "Politics"
    elif any(word in title_lower for word in ["covid", "health", "disease", "vaccine"]):
        return "Health"
    elif any(word in title_lower for word in ["stock", "business", "market", "economy"]):
        return "Business"
    elif any(word in title_lower for word in ["sport", "match", "tournament", "goal"]):
        return "Sports"
    return "Other"

def fetch_news():
    """Combine API + Scraped news into DataFrame."""
    news_items = fetch_api_news() + fetch_scraped_news()
    if not news_items:
        return pd.DataFrame(columns=["Title", "Source", "Sentiment Score", "Sentiment Label", "Topic", "Link"])
    
    df = pd.DataFrame(news_items)
    df["Sentiment Score"], df["Sentiment Label"] = zip(*df["title"].apply(analyze_sentiment))
    df["Topic"] = df["title"].apply(detect_topic)
    df.rename(columns={"title": "Title", "source": "Source", "link": "Link"}, inplace=True)
    return df

# ----------------------------
# UI
# ----------------------------
st.title("📰 Live News Dashboard")
st.caption("Auto-updates every 30 seconds with sentiment analysis")

# Inject JavaScript for auto-refresh every 30 seconds
st.markdown(
    """
    <script>
    setTimeout(function(){
        window.location.reload(1);
    }, 30000);
    </script>
    """,
    unsafe_allow_html=True
)

# Fetch latest news
news_df = fetch_news()

if news_df.empty:
    st.info("No news available right now.")
else:
    st.dataframe(news_df, use_container_width=True)

