import logging
import time
from bs4 import BeautifulSoup
from .base import BaseScraper

class BloombergScraper(BaseScraper):
    def __init__(self, config, db_handler, translator, feishu_handler):
        super().__init__(config, db_handler, translator, feishu_handler)
        self.source_conf = config["sources"]["bloomberg"]
        self.name = "Bloomberg"
        self.name_zh = self.source_conf["name_zh"]

    def scrape(self):
        url = self.source_conf["sitemap_url"]
        content = self.fetch(url)
        if not content:
            return

        try:
            soup = BeautifulSoup(content, 'xml')
            items = soup.find_all('url')
            
            for item in items:
                try:
                    loc = item.find('loc').text
                    
                    news = item.find('news:news')
                    if not news:
                        continue
                        
                    publication_date = news.find('news:publication_date').text
                    title = news.find('news:title').text
                    # Bloomberg sitemap might not always have keywords in news:keywords
                    keywords_tag = news.find('news:keywords')
                    keywords = keywords_tag.text if keywords_tag else ""
                    
                    if self.check_keywords(title) or self.check_keywords(keywords) or self.check_keywords(loc):
                        check_start = time.time()
                        exists = self.db_handler.url_exists(loc)
                        check_duration = time.time() - check_start

                        if exists:
                            continue
                        
                        logging.info(f"URL查重完成，耗时 {check_duration:.4f} 秒")
                        logging.info(f"发现新文章(原文): {title}")

                        start_time = time.time()
                        title_zh = self.translator.translate_title(title)
                        duration = time.time() - start_time
                        logging.info(f"标题翻译完成，耗时 {duration:.2f} 秒")
                        
                        # Convert keywords string to list for DB
                        keywords_list = [k.strip() for k in keywords.split(',')] if keywords else []
                        
                        is_new = self.db_handler.insert_news(
                            source=self.name_zh,
                            title_zh=title_zh,
                            title_en=title,
                            url=loc,
                            publish_time=publication_date,
                            keywords=keywords_list
                        )
                        
                        if is_new:
                            logging.info(f"新文章入库(中文): {title_zh}")
                            formatted_time = self.format_time(publication_date)
                            msg = f"{self.name_zh} ：{title_zh}\n{loc}\n{formatted_time}"
                            self.feishu_handler.to_feishu(self.feishu_handler.url, msg)

                except Exception as e:
                    logging.error(f"处理条目出错: {e}")
                    continue

        except Exception as e:
            logging.error(f"解析彭博社 Sitemap 出错: {e}")

