📰 Live News Dashboard
A real-time news dashboard built with Streamlit and PySpark that pulls headlines from:

Newsdata.io API

The Indian Express (web scraping)

The dashboard:

Updates every 30 seconds

Shows Sentiment Score and Sentiment Label

Detects Topic (Politics, Health, Business, etc.)

Includes clickable links to the full news articles

🚀 Features
Live Updates: Refreshes automatically every 30 seconds.

Multiple Sources: Combines API and web scraping.

Sentiment Analysis: Positive, Negative, Neutral scoring with TextBlob.

Topic Detection: Keyword-based classification.

PySpark + Pandas: Handles and displays data efficiently.

Streamlit UI: Interactive and easy to use.

📦 Requirements
Create a file requirements.txt with:

streamlit
pyspark
requests
beautifulsoup4
textblob

Install dependencies:

pip install -r requirements.txt

🛠 How It Works
Fetch Newsdata.io API → Retrieves top headlines in English.

Scrape The Indian Express → Grabs latest news headlines.

Analyze Sentiment → Uses TextBlob polarity.

Detect Topic → Keyword matching.

Display in Streamlit → DataFrame view, auto-refresh.

⚠️ Notes
Frequent API calls may hit rate limits on free plan.

If API is down, only scraped headlines will appear.

For deployment, consider hiding API key using Streamlit Secrets.

📜 License
MIT License — feel free to modify and share.

Copy
Edit
