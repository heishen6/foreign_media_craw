import requests
import logging
from abc import ABC, abstractmethod
from datetime import datetime

class BaseScraper(ABC):
    def __init__(self, config, db_handler, translator, feishu_handler):
        self.config = config
        self.db_handler = db_handler
        self.translator = translator
        self.feishu_handler = feishu_handler
        self.keywords = config.get("keywords", [])
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }

    def fetch(self, url):
        """获取URL内容"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.content
        except requests.Timeout:
            logging.error(f"请求超时: {url}")
            return None
        except requests.ConnectionError as e:
            logging.error(f"连接失败: {url} - {e}")
            return None
        except requests.HTTPError as e:
            logging.error(f"HTTP错误 {e.response.status_code}: {url}")
            return None
        except requests.RequestException as e:
            logging.error(f"请求异常: {url} - {e}")
            return None

    def check_keywords(self, text):
        if not text:
            return False
        for kw in self.keywords:
            if kw.lower() in text.lower():
                return True
        return False

    def format_time(self, iso_str):
        """格式化ISO时间字符串为本地时间"""
        try:
            # Handle 'Z' for UTC
            if iso_str.endswith('Z'):
                iso_str = iso_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(iso_str)
            # Convert to local time (controlled by TZ env var in main.py)
            return dt.astimezone(None).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError) as e:
            logging.debug(f"时间格式化失败，使用原始值: {iso_str} - {e}")
            return iso_str

    @abstractmethod
    def scrape(self):
        pass

