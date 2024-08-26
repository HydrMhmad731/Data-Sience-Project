import requests
from bs4 import BeautifulSoup
import json
from dataclasses import dataclass, asdict
from typing import List, Optional
import concurrent.futures
import re

@dataclass
class Article:
    url: str
    postid: str
    title: str
    keywords: List[str]
    thumbnail: Optional[str]
    published_time: Optional[str]
    last_updated: Optional[str]
    author: Optional[str]
    word_count: int
    video_duration: Optional[str]
    lang: Optional[str]
    description: Optional[str]
    classes: List[dict]
    full_text: Optional[str]

class ArticleScraper:
    def __init__(self):
        self.session = requests.Session()

    def fetch_article(self, url: str, article_limit_reached: List[bool]) -> Optional[Article]:
        if article_limit_reached[0]:
            return None

        print(f"Scraping: {url}...")
        response = self.session.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        if "video" in url or "image" in url:
            print(f"Skipped non-article content: {url}")
            return None

        script_tag = soup.find('script', type='text/tawsiyat')
        metadata = json.loads(script_tag.string) if script_tag else {}

        postid = self.extract_post_id(metadata, url)

        title = metadata.get('title') or self.extract_title_from_html(soup)
        keywords = [k.strip() for k in metadata.get('keywords', '').split(',')] if metadata.get('keywords') else self.extract_keywords_from_html(soup)
        thumbnail = metadata.get('thumbnail') or self.extract_thumbnail_from_html(soup)
        published_time = metadata.get('publication_date') or self.extract_date_from_html(soup, 'published_time')
        last_updated = metadata.get('last_updated_date') or self.extract_date_from_html(soup, 'last_updated')
        author = metadata.get('author') or self.extract_author_from_html(soup)
        word_count = int(metadata.get('word_count') or len(self.extract_full_text(soup).split()))
        video_duration = None  # Assuming no video data if not provided
        lang = metadata.get('language') or self.detect_language(soup)
        description = metadata.get('description') or self.extract_description_from_html(soup)
        classes = metadata.get('classes') or self.extract_classes_from_html(soup)
        full_text = self.extract_full_text(soup)

        if not all([title, published_time, author, description, full_text]):
            print(f"Skipping incomplete article: {url}")
            return None

        print(f"Finished: {url}")

        return Article(
            url=url,
            postid=postid,
            title=title,
            keywords=keywords,
            thumbnail=thumbnail,
            published_time=published_time,
            last_updated=last_updated,
            author=author,
            word_count=word_count,
            video_duration=video_duration,
            lang=lang,
            description=description,
            classes=classes,
            full_text=full_text
        )

    def extract_post_id(self, metadata: dict, url: str) -> str:
        postid = metadata.get('postid')
        if postid:
            return str(postid)

        match = re.search(r'/(\d+)\.html$', url)
        if match:
            return match.group(1)

        match = re.search(r'-p(\d+)$', url)
        if match:
            return match.group(1)

        return "0000"

    def extract_title_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        title_tag = soup.find('meta', {'property': 'og:title'})
        return title_tag['content'] if title_tag else None

    def extract_keywords_from_html(self, soup: BeautifulSoup) -> List[str]:
        keywords_tag = soup.find('meta', {'name': 'keywords'})
        if keywords_tag and keywords_tag['content']:
            return [k.strip() for k in keywords_tag['content'].split(',')]
        return []

    def extract_thumbnail_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        thumbnail_tag = soup.find('meta', {'property': 'og:image'})
        return thumbnail_tag['content'] if thumbnail_tag else None

    def extract_date_from_html(self, soup: BeautifulSoup, date_type: str) -> Optional[str]:
        if date_type == 'published_time':
            date_tag = soup.find('meta', {'property': 'article:published_time'})
        elif date_type == 'last_updated':
            date_tag = soup.find('meta', {'property': 'article:modified_time'})
        return date_tag['content'] if date_tag else None

    def extract_author_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        author_tag = soup.find('meta', {'name': 'author'})
        return author_tag['content'] if author_tag else None

    def extract_description_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        description_tag = soup.find('meta', {'property': 'og:description'})
        return description_tag['content'] if description_tag else None

    def extract_classes_from_html(self, soup: BeautifulSoup) -> List[dict]:
        class_tags = soup.find_all('meta', {'name': 'classification'})
        if class_tags:
            return [{'class_name': tag['content']} for tag in class_tags]
        return []

    def detect_language(self, soup: BeautifulSoup) -> Optional[str]:
        lang_tag = soup.find('html')
        return lang_tag.get('lang') if lang_tag else None

    def extract_full_text(self, soup: BeautifulSoup) -> Optional[str]:
        paragraphs = [p.get_text() for p in soup.find_all('p')]
        return '\n'.join(paragraphs) if paragraphs else None

    def parse_sitemap(self, sitemap_url: str) -> List[str]:
        response = self.session.get(sitemap_url)
        soup = BeautifulSoup(response.content, 'xml')
        urls = [loc.get_text() for loc in soup.find_all('loc')]
        return urls

class FileUtility:
    @staticmethod
    def save_articles(articles: List[Article], year: int, month: int):
        filename = f"articles_{year}_{month:02d}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            articles_dict = [asdict(article) for article in articles]
            json.dump(articles_dict, f, ensure_ascii=False, indent=4)

def main(year: int, article_limit: int):
    article_scraper = ArticleScraper()
    total_articles = 0
    article_limit_reached = [False]

    main_sitemap_url = "https://www.almayadeen.net/sitemaps/all.xml"
    monthly_sitemaps = article_scraper.parse_sitemap(main_sitemap_url)

    relevant_sitemaps = [
        sitemap for sitemap in monthly_sitemaps
        if int(sitemap.split('-')[-2]) == year
    ]

    for sitemap_url in relevant_sitemaps:
        if total_articles >= article_limit:
            print(f"Article limit of {article_limit} reached. Stopping scraping.")
            break

        month = int(sitemap_url.split('-')[-1].replace('.xml', ''))
        all_articles = []

        article_urls = article_scraper.parse_sitemap(sitemap_url)
        print(f"Found {len(article_urls)} article URLs for {year}-{month:02d}. Starting to scrape...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(article_scraper.fetch_article, url, article_limit_reached): url for url in article_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                if article_limit_reached[0]:
                    break
                try:
                    article = future.result()
                    if article:
                        all_articles.append(article)
                        total_articles += 1
                        if total_articles >= article_limit:
                            print(f"Article limit of {article_limit} reached. Stopping scraping.")
                            article_limit_reached[0] = True
                            break
                except Exception as e:
                    print(f"Error fetching article from {future_to_url[future]}: {e}")

        if all_articles:
            FileUtility.save_articles(all_articles, year, month)
            print(f"Scraping is complete for {year}-{month:02d}. {len(all_articles)} articles saved.")

    print(f"Total {total_articles} articles scraped and saved.")

if __name__ == "__main__":
    year = 2024  # Set the year to scrape
    article_limit = 11000  # Set the total number of articles to scrape
    main(year, article_limit)
