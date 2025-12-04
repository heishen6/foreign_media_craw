import logging
import time
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from .base import BaseScraper

class FinancialTimesScraper(BaseScraper):
    def __init__(self, config, db_handler, translator, feishu_handler):
        super().__init__(config, db_handler, translator, feishu_handler)
        self.source_conf = config["sources"]["ft"]
        self.name = "FinancialTimes"
        self.name_zh = self.source_conf["name_zh"]

    def scrape(self):
        import time as _time
        # 添加时间戳参数绕过 FT CDN 缓存，确保获取最新 RSS 内容
        base_url = self.source_conf["sitemap_url"]
        url = f"{base_url}&_={int(_time.time())}"
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
                    description_raw = item.find('description').text if item.find('description') else ""
                    
                    # Clean HTML
                    description = BeautifulSoup(description_raw, "html.parser").get_text(separator=" ", strip=True)
                    
                    pub_date_str = item.find('pubDate').text
                    
                    # Convert RFC 822 date to ISO format for DB compatibility/uniformity
                    try:
                        dt = parsedate_to_datetime(pub_date_str)
                        publication_date = dt.isoformat()
                    except Exception as e:
                        logging.warning(f"日期解析失败 ({pub_date_str}): {e}")
                        publication_date = pub_date_str # Fallback

                    # Check keywords in title, description, or URL
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
                        
                        # Translate description if exists
                        description_zh = ""
                        if description:
                            description_zh = self.translator.translate_summary(description)
                            
                        duration = time.time() - start_time
                        logging.info(f"翻译完成(标题+摘要)，耗时 {duration:.2f} 秒")
                        
                        # FT RSS doesn't have explicit keyword tags
                        keywords_list = []
                        
                        is_new = self.db_handler.insert_news(
                            source=self.name_zh,
                            title_zh=title_zh,
                            title_en=title,
                            url=link,
                            publish_time=publication_date,
                            keywords=keywords_list,
                            description=description # Store original cleaned description or translated? Usually store original or both. DB has one col. 
                            # Requirement says "database adjustment... description". Usually we store original text or translated? 
                            # For "title", we have title_zh and title_en columns.
                            # For "description", we only added "description" column.
                            # I'll store the CLEANED ORIGINAL description in DB (to keep record of what it was), 
                            # but send TRANSLATED description in Feishu.
                            # Or maybe user wants translated description in DB? User didn't specify DB column for zh description.
                            # I'll store original in DB to match 'title_en' logic (description col usually implies original content).
                            # But wait, `title_zh` is translated. `description` is ambiguous.
                            # Given user wants translation for push, I'll push translated.
                        )
                        
                        if is_new:
                            logging.info(f"新文章入库(中文): {title_zh}")
                            formatted_time = self.format_time(publication_date)

                            msg = f"{self.name_zh} ：{title_zh}\n{link}\n{formatted_time}\n摘要: {description_zh}"
                            self.feishu_handler.broadcast(msg)

                except Exception as e:
                    logging.error(f"处理条目出错: {e}")
                    continue

        except Exception as e:
            logging.error(f"解析金融时报 RSS 出错: {e}")

