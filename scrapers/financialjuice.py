import logging
import time
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from .base import BaseScraper

class FinancialJuiceScraper(BaseScraper):
    def __init__(self, config, db_handler, translator, feishu_handler):
        super().__init__(config, db_handler, translator, feishu_handler)
        self.source_conf = config["sources"]["financialjuice"]
        self.name = "FinancialJuice"
        self.name_zh = self.source_conf["name_zh"]

    def scrape(self):
        url = self.source_conf["sitemap_url"]
        content = self.fetch(url)
        if not content:
            return

        try:
            soup = BeautifulSoup(content, 'xml')
            items = soup.find_all('item')
            
            for item in items:
                try:
                    title = item.find('title').text
                    link = item.find('link').text
                    # Try to get description, handle if empty or missing
                    description_tag = item.find('description')
                    description_raw = description_tag.text if description_tag else ""
                    
                    # Clean HTML
                    description = BeautifulSoup(description_raw, "html.parser").get_text(separator=" ", strip=True)
                    
                    pub_date_str = item.find('pubDate').text
                    
                    try:
                        dt = parsedate_to_datetime(pub_date_str)
                        publication_date = dt.isoformat()
                    except Exception as e:
                        logging.warning(f"日期解析失败 ({pub_date_str}): {e}")
                        publication_date = pub_date_str

                    # Check keywords
                    if self.check_keywords(title) or self.check_keywords(description) or self.check_keywords(link):
                        check_start = time.time()
                        exists = self.db_handler.url_exists(link)
                        check_duration = time.time() - check_start

                        if exists:
                            continue
                        
                        logging.info(f"URL查重完成，耗时 {check_duration:.4f} 秒")
                        logging.info(f"发现新文章(原文): {title}")

                        start_time = time.time()
                        title_zh = self.translator.translate_title(title)
                        
                        # Translate description
                        description_zh = ""
                        if description:
                            description_zh = self.translator.translate_summary(description)

                        duration = time.time() - start_time
                        logging.info(f"翻译完成(标题+摘要)，耗时 {duration:.2f} 秒")
                        
                        keywords_list = []
                        
                        is_new = self.db_handler.insert_news(
                            source=self.name_zh,
                            title_zh=title_zh,
                            title_en=title,
                            url=link,
                            publish_time=publication_date,
                            keywords=keywords_list,
                            description=description
                        )
                        
                        if is_new:
                            logging.info(f"新文章入库(中文): {title_zh}")
                            formatted_time = self.format_time(publication_date)
                            # Include description in notification if present
                            # User requested NO URL for FinancialJuice
                            desc_snippet = f"\n摘要: {description_zh}" if description_zh else ""
                            msg = f"{self.name_zh} ：{title_zh}\n{formatted_time}{desc_snippet}"
                            self.feishu_handler.to_feishu(self.feishu_handler.url, msg)

                except Exception as e:
                    logging.error(f"处理条目出错: {e}")
                    continue

        except Exception as e:
            logging.error(f"解析 FinancialJuice RSS 出错: {e}")

