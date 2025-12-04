import time
import json
import logging
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from utils.logger import setup_logging
from utils.translation import Translator
from db import DBHandler
from tools.feishu import FeishuHandler
from scrapers.reuters import ReutersScraper
from scrapers.bloomberg import BloombergScraper
from scrapers.ft import FinancialTimesScraper
from scrapers.financialjuice import FinancialJuiceScraper

# Add tools directory to path to allow imports if needed, though we import relatively or directly
sys.path.append(os.path.join(os.path.dirname(__file__), 'tools'))

def load_config():
    """加载配置文件"""
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("配置文件 config.json 不存在")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"配置文件格式错误: {e}")
        raise
    except PermissionError:
        logging.error("无权限读取配置文件 config.json")
        raise

def run_scraper(scraper):
    """运行单个爬虫的辅助函数"""
    import requests
    try:
        logging.debug(f"正在运行 {scraper.name_zh} 爬虫...")
        scraper.scrape()
        logging.debug(f"{scraper.name_zh} 爬虫运行完成")
        return True
    except requests.RequestException as e:
        logging.error(f"{scraper.name_zh} 网络请求失败: {e}")
        return False
    except psycopg2.Error as e:
        logging.error(f"{scraper.name_zh} 数据库操作失败: {e}")
        return False
    except Exception as e:
        # 其他未预期的异常，记录详细信息
        logging.error(f"{scraper.name_zh} 爬虫执行异常: {type(e).__name__}: {e}", exc_info=True)
        return False

def check_db_health(db_handler):
    """检查数据库连接健康状态"""
    try:
        with db_handler.get_connection() as conn:
            if not conn:
                logging.warning("数据库连接池无可用连接")
                return False
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return True
    except psycopg2.Error as e:
        logging.error(f"数据库健康检查失败: {e}")
        return False

def main():
    # Set timezone to China Standard Time
    os.environ['TZ'] = 'Asia/Shanghai'
    try:
        time.tzset()
    except AttributeError:
        # Windows doesn't support tzset
        pass

    setup_logging()
    logging.info("正在启动爬虫服务...")

    # 加载配置
    try:
        config = load_config()
    except (FileNotFoundError, json.JSONDecodeError, PermissionError):
        return

    # 初始化各个组件
    db_handler = None
    try:
        db_handler = DBHandler(config["db"])
        translator = Translator(config["llm"])

        # Initialize Feishu handler with webhook URLs from config
        feishu_handler = FeishuHandler(config["feishu_webhooks"])
    except psycopg2.Error as e:
        logging.error(f"数据库初始化失败: {e}")
        return
    except KeyError as e:
        logging.error(f"配置文件缺少必要字段: {e}")
        return

    scrapers = [
        ReutersScraper(config, db_handler, translator, feishu_handler),
        BloombergScraper(config, db_handler, translator, feishu_handler),
        FinancialTimesScraper(config, db_handler, translator, feishu_handler),
        FinancialJuiceScraper(config, db_handler, translator, feishu_handler)
    ]

    logging.info("服务初始化完成，开始循环任务...")
    logging.info(f"使用线程池并发执行 {len(scrapers)} 个爬虫")

    last_heartbeat = time.time()
    last_health_check = time.time()
    health_check_interval = 300  # 每5分钟检查一次数据库健康状态

    try:
        # 创建线程池一次，在整个生命周期内重复使用
        with ThreadPoolExecutor(max_workers=len(scrapers)) as executor:
            while True:
                # 定期检查数据库健康状态
                if time.time() - last_health_check > health_check_interval:
                    if check_db_health(db_handler):
                        logging.info("数据库健康检查: 正常")
                    else:
                        logging.warning("数据库健康检查: 异常，连接池可能需要重建")
                    last_health_check = time.time()

                # 使用线程池并发执行所有爬虫
                # 提交所有爬虫任务
                future_to_scraper = {executor.submit(run_scraper, scraper): scraper for scraper in scrapers}

                # 等待所有任务完成
                for future in as_completed(future_to_scraper):
                    scraper = future_to_scraper[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        # 这里的异常应该已经在run_scraper中处理过了
                        # 这是防御性代码，记录堆栈信息以便调试
                        logging.error(f"{scraper.name_zh} 线程池执行异常: {type(e).__name__}: {e}", exc_info=True)

                # Print heartbeat every 60 seconds to let user know service is alive
                if time.time() - last_heartbeat > 60:
                    logging.info("服务运行正常，正在持续监控中...")
                    last_heartbeat = time.time()

                time.sleep(config["update_interval_seconds"])

    except KeyboardInterrupt:
        logging.info("用户停止服务。")
    except SystemExit:
        logging.info("系统退出信号。")
        raise
    except psycopg2.Error as e:
        logging.critical(f"数据库致命错误: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"未预期的致命错误: {type(e).__name__}: {e}", exc_info=True)
    finally:
        if db_handler:
            try:
                db_handler.close()
            except Exception as e:
                logging.error(f"关闭数据库连接时出错: {e}")

if __name__ == "__main__":
    main()

