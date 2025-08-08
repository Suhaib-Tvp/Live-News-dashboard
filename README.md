# 📰 Live News Dashboard

A real-time news dashboard built with **Streamlit** and **PySpark** that pulls headlines from:

- **Newsdata.io API**
- **The Indian Express** (web scraping)

The dashboard:
- Updates every **30 seconds**
- Shows **Sentiment Score** and **Sentiment Label**
- Detects **Topic** (Politics, Health, Business, etc.)
- Includes clickable links to the full news articles

---

## 🚀 Features

- **Live Updates**: Refreshes automatically every 30 seconds  
- **Multiple Sources**: Combines API and web scraping  
- **Sentiment Analysis**: Positive, Negative, Neutral scoring with `TextBlob`  
- **Topic Detection**: Keyword-based classification  
- **PySpark + Pandas**: Handles and displays data efficiently  
- **Streamlit UI**: Interactive and easy to use  

---

## 📦 Requirements

Create a file called `requirements.txt` with the following:

```
streamlit
pyspark
requests
beautifulsoup4
textblob
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🛠 How It Works

1. **Fetch Newsdata.io API** → Retrieves top headlines in English  
2. **Scrape The Indian Express** → Grabs latest news headlines  
3. **Analyze Sentiment** → Uses `TextBlob` polarity  
4. **Detect Topic** → Keyword matching  
5. **Display in Streamlit** → DataFrame view, auto-refresh  

---

## ▶️ Run the App

```bash
streamlit run app.py
```

This will open the dashboard in your browser at:

```
http://localhost:8501
```

---

## ⚠️ Notes

- Frequent API calls may hit **rate limits** on the free plan  
- If the API is down, only scraped headlines will appear  
- For deployment, keep your API key safe using **Streamlit Secrets**  

---

## 📜 License

MIT License — feel free to modify and share.
