# Foreign Media Crawler

外媒新闻爬虫系统，用于抓取国际主流财经媒体的最新资讯。

## 功能特点

- 支持多个主流财经媒体网站爬取：
  - Reuters (路透社)
  - Bloomberg (彭博社)
  - Financial Times (金融时报)
  - Financial Juice (金融新闻)

- 自动翻译：使用 LLM 进行内容翻译
- 飞书通知：新资讯自动推送到飞书群
- 数据库存储：PostgreSQL 数据持久化
- 多线程并发：高效的并发爬取
- 健康检查：定期数据库健康检查

## 安装

1. 克隆项目：
```bash
git clone <your-repo-url>
cd foreign_media_craw
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置 `config.json`：
```json
{
  "db": {
    "host": "your-db-host",
    "port": 5432,
    "user": "your-user",
    "password": "your-password",
    "database": "your-database"
  },
  "llm": {
    "api_key": "your-api-key",
    "model": "your-model"
  },
  "feishu_webhook": "your-webhook-url",
  "update_interval_seconds": 300
}
```

## 使用

运行爬虫服务：
```bash
python main.py
```

## 项目结构

```
foreign_media_craw/
├── main.py              # 主程序入口
├── db.py                # 数据库操作
├── config.json          # 配置文件
├── requirements.txt     # 依赖列表
├── scrapers/            # 爬虫模块
│   ├── reuters.py
│   ├── bloomberg.py
│   ├── ft.py
│   └── financialjuice.py
├── tools/               # 工具模块
│   └── feishu.py
└── utils/               # 工具函数
    ├── logger.py
    └── translation.py
```

## 许可证

MIT License

