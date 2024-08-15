import requests
from bs4 import BeautifulSoup
import json
import os # A module to used to use the operating system, like creating directories...
from dataclasses import dataclass, asdict
from typing import List
import concurrent.futures # A mmodule to execute code in parallel  to help speed up the process


@dataclass
class Article:
    url: str
    post_id: str
    title: str
    keywords: List[str]
    thumbnail: str
    publication_date: str
    last_updated_date: str
    author: str
    video_duration: str  #
    word_count: int  # Word count of the article
    lang: str  # Language of the article
    description: str  # Description of the article
    classes: List[str]  # Article classes as an array
    full_text: str


class SitemapParser: #A Class to handle the extraction of sitemap urls
    @staticmethod
    def get_monthly_sitemaps(main_sitemap_url: str) -> List[str]:# To fetch main sitemap, and extract urls for monthly sitemaps
        print(f"Fetching main sitemap index from: {main_sitemap_url}")

        response = requests.get(main_sitemap_url)#Fetch the content of the main   sitemap
        soup = BeautifulSoup(response.content, 'lxml')#Parses html/xml content by using lxml parser
        sitemap_tags = soup.find_all('loc')#to find all <loc> tags for the urls of monthly sitemap
        sitemap_urls = [tag.text for tag in sitemap_tags]#Extract the text from each <loc> and store in list
        print(f"Found {len(sitemap_urls)} monthly sitemaps.")

        return sitemap_urls#return the list of monthly sitemap

    @staticmethod
    def get_article_urls(sitemap_url: str) -> List[str]:#Fetches monthly sitemap to get urls for individual articles
        print(f"Fetching sitemap from: {sitemap_url}")

        response = requests.get(sitemap_url)
        soup = BeautifulSoup(response.content, 'lxml')
        article_tags = soup.find_all('loc')
        article_urls = [tag.text for tag in article_tags]
        print(f"Found {len(article_urls)} articles in the sitemap.")

        return article_urls


class ArticleScraper: #A Class for scraping the content of a single article
    def __init__(self, article_url: str):
        self.article_url = article_url

    def scrape_article(self) -> Article:#Used to scrape article page to extract information(url, author...)
        print(f"Scraping article: {self.article_url}")

        response = requests.get(self.article_url)#fetch the content of article webpage
        soup = BeautifulSoup(response.content, 'lxml')

        script_tag = soup.find('script', {'type': 'application/ld+json'})#find a script that contains structured data about the article
        if script_tag:
            try:
                metadata = json.loads(script_tag.string)#parses json data int python dict
            except json.JSONDecodeError:
                metadata = {}
        else:
            metadata = {}

        # Safely get metadata values with default fallbacks
        url = metadata.get('url', self.article_url)
        post_id = metadata.get('identifier', 'unknown')
        title = metadata.get('headline', 'unknown')
        keywords = metadata.get('keywords', [])
        if isinstance(keywords, str):
            keywords = keywords.split(',')
        thumbnail = metadata.get('image', {}).get('url', 'unknown') \
            if isinstance(metadata.get('image'), dict) else 'unknown'
        publication_date = metadata.get('datePublished', 'unknown')
        last_updated_date = metadata.get('dateModified', 'unknown')
        author = metadata.get('author', {}).get('name', 'unknown') \
            if isinstance(metadata.get('author'), dict) else 'unknown'
        video_duration = metadata.get('duration', 'unknown')
        word_count = len(soup.get_text().split())
        lang = metadata.get('inLanguage', 'unknown')
        description = metadata.get('description', 'unknown')

        classes = metadata.get('articleSection', [])
        if isinstance(classes, str):
            classes = classes.split(',')  # Ensure classes are a list of strings

        paragraphs = soup.find_all('p')  # Extract all <p> tags from the article
        full_text = "\n".join([p.get_text() for p in paragraphs])  # Combine all paragraphs to form the full text

        article = Article(
            url=url,
            post_id=post_id,
            title=title,
            keywords=keywords,
            thumbnail=thumbnail,
            publication_date=publication_date,
            last_updated_date=last_updated_date,
            author=author,
            video_duration=video_duration,
            word_count=word_count,
            lang=lang,
            description=description,
            classes=classes,
            full_text=full_text
        )

        print(f"Finished scraping article: {self.article_url}")
        return article


class FileUtility: #A Class for saving scraped articles to json file
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def save_articles(self, articles: List[Article], year: int, month: int):#Save the list Article to a json file
        os.makedirs(self.output_dir, exist_ok=True)#to create the output directory
        file_name = f"articles_{year}_{month:02}.json"#generate the filename based on tyear and month
        file_path = os.path.join(self.output_dir, file_name)
        articles_dict = [asdict(article) for article in articles]
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(articles_dict, json_file, ensure_ascii=False, indent=4)#saves the list of dict to json file with formatting
        print(f"Saved {len(articles)} articles to {file_path}")


def process_sitemaps(main_sitemap_url: str, article_limit: int):#Make the flow of process easy, and smooth like
    sitemap_parser = SitemapParser()
    file_utility = FileUtility(output_dir="scraped_articles")


    monthly_sitemaps = sitemap_parser.get_monthly_sitemaps(main_sitemap_url=main_sitemap_url)#fetch the list of monthly sitemap

    total_articles_scraped = 0

    for sitemap_url in monthly_sitemaps:
        if total_articles_scraped >= article_limit:
            break

        # Extract year and month from the sitemap URL
        year_month = sitemap_url.split('/')[-1].replace('sitemap-', '').replace('.xml', '').split('-')
        year = int(year_month[0])
        month = int(year_month[1])

        print(f"Processing month: {year}-{month:02d}")

        # Get article URLs from the monthly sitemap
        article_urls = sitemap_parser.get_article_urls(sitemap_url=sitemap_url)

        # Scrape articles with parallel processing
        articles = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:#scrapes multiple articles in parallel
            future_to_article = {executor.submit(ArticleScraper(article_url).scrape_article): article_url for
                                 article_url in article_urls}
            for future in concurrent.futures.as_completed(future_to_article):
                try:
                    article = future.result()
                    articles.append(article)
                    total_articles_scraped += 1#keeps track of the total number of articles scraped so far
                    if total_articles_scraped >= article_limit:
                        break
                except Exception as e:
                    print(f"Error scraping article: {e}")

        # Save the articles for the current month
        if articles:
            file_utility.save_articles(articles=articles, year=year, month=month)
            print(f"Total number of articles scraped so far: {total_articles_scraped}")


if __name__ == "__main__":#The main point of script when it runs
    # Process all months from the main sitemap index and limit to 13,000 articles
    main_sitemap_url = "https://www.almayadeen.net/sitemaps/all.xml"
    process_sitemaps(main_sitemap_url=main_sitemap_url, article_limit=13000)
