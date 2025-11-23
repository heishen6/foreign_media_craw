import psycopg2
import logging
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

class DBHandler:
    def __init__(self, config):
        self.config = config
        self.pool = None
        self.create_pool()
        self.init_db()

    def create_pool(self):
        """创建线程安全的连接池"""
        try:
            # ThreadedConnectionPool 适用于多线程环境
            # minconn: 最小连接数，maxconn: 最大连接数
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,  # 根据实际并发数调整，当前4个爬虫可设为10
                user=self.config["user"],
                password=self.config["password"],
                host=self.config["host"],
                port=self.config["port"],
                dbname=self.config["dbname"]
            )
            logging.info("数据库连接池已创建。")
        except psycopg2.OperationalError as e:
            logging.error(f"数据库连接失败 (检查主机/端口/凭据): {e}")
            self.pool = None
            raise
        except psycopg2.Error as e:
            logging.error(f"创建数据库连接池失败: {e}")
            self.pool = None
            raise

    @contextmanager
    def get_connection(self):
        """从连接池获取连接的上下文管理器，确保连接使用后归还"""
        if not self.pool:
            logging.error("连接池未初始化")
            yield None
            return

        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except psycopg2.pool.PoolError as e:
            logging.error(f"连接池已耗尽或出错: {e}")
            yield None
        except psycopg2.Error as e:
            logging.error(f"获取数据库连接失败: {e}")
            yield None
        finally:
            if conn:
                try:
                    self.pool.putconn(conn)
                except psycopg2.Error as e:
                    logging.error(f"归还连接到连接池失败: {e}")

    def init_db(self):
        """初始化数据库表结构"""
        if not self.pool:
            logging.error("无法初始化数据库: 连接池未创建")
            return

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.config['table_name']} (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            title_zh TEXT,
            title_en TEXT,
            url TEXT UNIQUE NOT NULL,
            publish_time TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            keywords TEXT[],
            description TEXT
        );
        """
        try:
            with self.get_connection() as conn:
                if not conn:
                    logging.error("无法初始化数据库: 无法获取连接")
                    return

                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    # Add description column if it doesn't exist (for existing tables)
                    try:
                        cursor.execute(f"ALTER TABLE {self.config['table_name']} ADD COLUMN IF NOT EXISTS description TEXT;")
                    except psycopg2.ProgrammingError as e:
                        # Column already exists or SQL syntax issue
                        logging.debug(f"添加 description 列跳过: {e}")
                        conn.rollback()

                    conn.commit()
                    logging.info(f"表 {self.config['table_name']} 检查/创建成功。")
        except psycopg2.Error as e:
            logging.error(f"创建表失败: {e}", exc_info=True)
            raise

    def insert_news(self, source, title_zh, title_en, url, publish_time, keywords=None, description=None):
        """插入新闻数据，使用连接池"""
        if not self.pool:
            logging.warning("插入失败: 连接池未初始化")
            return False

        # Ensure keywords is a list for TEXT[] column
        if keywords is None:
            keywords = []
        elif isinstance(keywords, str):
            if keywords.strip() == "":
                keywords = []
            else:
                # Split by comma if it looks like a CSV, otherwise simple list
                keywords = [k.strip() for k in keywords.split(',')] if ',' in keywords else [keywords]

        query = f"""
        INSERT INTO {self.config['table_name']} (source, title_zh, title_en, url, publish_time, keywords, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
        """
        try:
            with self.get_connection() as conn:
                if not conn:
                    logging.warning("插入失败: 无法获取数据库连接")
                    return False

                with conn.cursor() as cursor:
                    cursor.execute(query, (source, title_zh, title_en, url, publish_time, keywords, description))
                    row = cursor.fetchone()
                    conn.commit()
                    if row:
                        return True
                    else:
                        return False  # Duplicate
        except psycopg2.IntegrityError as e:
            # 唯一约束冲突（URL重复）
            logging.debug(f"URL已存在，跳过插入: {url}")
            return False
        except psycopg2.DataError as e:
            # 数据类型错误
            logging.error(f"数据格式错误: {e}")
            return False
        except psycopg2.Error as e:
            logging.error(f"插入新闻失败: {e}")
            return False

    def url_exists(self, url):
        """检查URL是否存在，使用连接池"""
        if not self.pool:
            logging.warning("查询失败: 连接池未初始化")
            return False

        query = f"SELECT 1 FROM {self.config['table_name']} WHERE url = %s"
        try:
            with self.get_connection() as conn:
                if not conn:
                    logging.warning("查询失败: 无法获取数据库连接")
                    return False

                with conn.cursor() as cursor:
                    cursor.execute(query, (url,))
                    return cursor.fetchone() is not None
        except psycopg2.Error as e:
            logging.error(f"检查URL是否存在失败: {e}")
            return False

    def close(self):
        """关闭连接池，释放所有连接"""
        if self.pool:
            try:
                self.pool.closeall()
                logging.info("数据库连接池已关闭。")
            except psycopg2.Error as e:
                logging.error(f"关闭连接池时出错: {e}")

