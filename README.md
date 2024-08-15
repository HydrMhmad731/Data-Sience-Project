# Al Mayadeen Web Scraping
This project is a web scraper designed to extract and save articles from the Al Mayadeen website. It processes sitemaps to gather URLs for individual articles, scrapes content such as titles, keywords, and full text, and saves the results in organized JSON files.

## Features
- **Scrapes Metadata:** Extracts information like URLs, post IDs, titles, keywords, thumbnails, publication dates, last updated dates, authors, video durations, word counts, languages, descriptions, and article classes.
- **Parallel Processing:** Uses multithreading to speed up the scraping of multiple articles simultaneously.
- **Structured Data Storage:** Saves scraped articles in JSON format, organized by year and month.

## How to Use

1. **Run the Script:** The script automatically processes all months from the main sitemap and limits the scraping to a specified number of articles.
2. **Output:** Scraped articles are saved in the `scraped_articles` directory, organized into JSON files.
