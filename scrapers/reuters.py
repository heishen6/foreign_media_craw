import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from .base import BaseScraper
from bs4 import BeautifulSoup

class ReutersScraper(BaseScraper):
    def __init__(self, config, db_handler, translator, feishu_handler):
        super().__init__(config, db_handler, translator, feishu_handler)
        self.source_conf = config["sources"]["reuters"]
        self.name = "Reuters"
        self.name_zh = self.source_conf["name_zh"]

    def scrape(self):
        # 添加时间戳参数绕过 CDN 缓存
        base_url = self.source_conf["sitemap_url"]
        url = f"{base_url}&_={int(time.time())}"
        content = self.fetch(url)
        if not content:
            return

        try:
            # Reuters sitemap index usually points to sub-sitemaps.
            # But let's try to parse it. If it has <sitemap> tags, we follow them.
            # If it has <url> tags, we process them.

            soup = BeautifulSoup(content, 'xml')

            # Check if it is a sitemap index
            sitemaps = soup.find_all('sitemap')
            if sitemaps:
                # Reuters使用分页式sitemap结构，按时间倒序排列
                # sitemaps[0] 包含最新的新闻（已通过实际测试验证）
                # 所有子sitemap的lastmod时间相同，因此无需比较lastmod
                target_sitemap_url = sitemaps[0].find('loc').text
                # 子sitemap也加时间戳
                target_sitemap_url = f"{target_sitemap_url}&_={int(time.time())}"
                logging.debug(f"路透社: 正在获取最新sitemap: {target_sitemap_url}")
                content = self.fetch(target_sitemap_url)
                if not content:
                    return
                soup = BeautifulSoup(content, 'xml')

            items = soup.find_all('url')
            for item in items:
                try:
                    loc = item.find('loc').text
                    
                    # Parse news specific tags
                    news = item.find('news:news')
                    if not news:
                        continue
                        
                    publication_date = news.find('news:publication_date').text
                    title = news.find('news:title').text
                    keywords = news.find('news:keywords').text if news.find('news:keywords') else ""
                    
                    # Check keywords in title, news keywords, or URL
                    if self.check_keywords(title) or self.check_keywords(keywords) or self.check_keywords(loc):
                        check_start = time.time()
                        exists = self.db_handler.url_exists(loc)
                        check_duration = time.time() - check_start

                        if exists:
                            continue

                        logging.info(f"URL查重完成，耗时 {check_duration:.4f} 秒")
                        logging.info(f"发现新文章(原文): {title}")

                        # Translate
                        start_time = time.time()
                        title_zh = self.translator.translate_title(title)
                        duration = time.time() - start_time
                        logging.info(f"标题翻译完成，耗时 {duration:.2f} 秒")
                        
                        # Convert keywords string to list for DB
                        keywords_list = [k.strip() for k in keywords.split(',')] if keywords else []

                        # Insert
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
                            # Notification
                            formatted_time = self.format_time(publication_date)
                            msg = f"{self.name_zh} ：{title_zh}\n{loc}\n{formatted_time}"
                            self.feishu_handler.to_feishu(self.feishu_handler.url, msg)
                            
                except (AttributeError, KeyError) as e:
                    logging.error(f"解析条目数据结构错误: {e}")
                    continue
                except Exception as e:
                    logging.error(f"处理条目时未预期错误: {type(e).__name__}: {e}", exc_info=True)
                    continue

        except Exception as e:
            logging.error(f"解析路透社 Sitemap 出错: {type(e).__name__}: {e}", exc_info=True)

