"""
使用方法：
from tools.feishu import FeishuHandler

FeishuHandler.to_feishu(FEISHU_WEBHOOK, message)

"""


import time
import json
import requests
import logging
import logging.handlers
import traceback
from requests_toolbelt import MultipartEncoder

AT_ALL = "<at user_id=\"all\">所有人</at>"


class FeishuHandler(logging.Handler):
    def __init__(self, url, mute=False):
        super().__init__()
        self.url = url
        self.mute = mute
    
    @classmethod
    def to_feishu(cls, url, msg):
        payload_message = {
            "msg_type": "text",
            "content": {
                "text": msg
            }
        }
        headers = {
            "Content-Type": "application/json"
        }
        max_retry = 5
        retry = 0
        while retry < max_retry:
            try:
                requests.request("POST", url, headers=headers, data=json.dumps(payload_message), timeout=10)
                break
            except BaseException as e:
                # traceback.print_exc()
                print(f"Feishu to {url} failed [{retry}/{max_retry}] times, retrying")
                time.sleep(1)
                continue
        
    def emit(self, msg):
        msg = self.format(msg)
        FeishuHandler.msg(self.url, msg)
    
    def msg(self, msg):
        if self.mute:
            return 
        FeishuHandler.to_feishu(self.url, msg)
    

def upload_img(web_hook, img_path=None):
    app_id = "cli_a5bc813a976cd00c"
    app_secret = "x1bkGsown5qrrMGLJ5NO5o6PDGTPrRL8"
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    payload_data = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    response = requests.post(url=url, data=json.dumps(payload_data), headers=headers).json()
    token = response["tenant_access_token"]
    image_key_headers = {
        "Authorization": "Bearer " + token
    }
    get_image_url = "https://open.feishu.cn/open-apis/im/v1/images"
    form = {
        "image_type": "message",
        "image": (open(img_path, "rb"))
    }
    multi_form = MultipartEncoder(form)
    image_key_headers["Content-Type"] = multi_form.content_type
    response = requests.request("POST", get_image_url, headers=image_key_headers, data=multi_form).json()
    image_key = response["data"]["image_key"]
    
    form = {
        "msg_type": "image",
        "content": {"image_key": image_key}
    }
    print(image_key)
    headers = {
        "Authorization": "Bearer " + token
    }
    response = requests.post(url=web_hook, data=json.dumps(form), headers=headers)
    print(response)


def upload_file(chat_id, file_path, file_name=None):
    app_id = "cli_a5bc813a976cd00c"
    app_secret = "x1bkGsown5qrrMGLJ5NO5o6PDGTPrRL8"
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    payload_data = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    response = requests.post(url=url, data=json.dumps(payload_data), headers=headers).json()
    token = response["tenant_access_token"]
    if not file_name:
        file_name = file_path.split(",")[0]
    # 上传到飞书
    url = "https://open.feishu.cn/open-apis/im/v1/files"
    form = {"file_type": "pdf",
            "file_name": file_name,
            "file": (file_name, open(file_path, "rb"), "text/plain")}  
    multi_form = MultipartEncoder(form)
    headers = {"Authorization": f"Bearer {token}"}
    headers["Content-Type"] = multi_form.content_type
    response = requests.request("POST", url, headers=headers, data=multi_form)
    file_key = response.json().get("data").get("file_key")
    form = {
        "receive_id": chat_id,
        "msg_type": "file",
        "content": json.dumps({"file_key": file_key})
    }
    # 发送到群
    headers = {
        "Authorization": "Bearer " + token
    }
    robot_url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    response = requests.post(url=robot_url, data=json.dumps(form), headers=headers)
    print(response)

# if __name__ == "__main__":
#     FeishuHandler.to_feishu("https://open.feishu.cn/open-apis/bot/v2/hook/2514dacb-37d9-4eca-a41a-1702b79a2192", "test")