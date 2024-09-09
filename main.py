import httpx
import asyncio
import os
import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from common import common
from db import crdb
from typing import Any


url = "https://www.douyin.com/aweme/v1/web/comment/list/"
reply_url = url + "reply/"

def setup_logging(logs_dir: str):
    # Configure logging
    os.makedirs(logs_dir, exist_ok=True)
    log_filename = f"{logs_dir}/app.log"  # or path to the log file
    handler = TimedRotatingFileHandler(
        log_filename,
        when="midnight",     # Rotate at midnight
        interval=1,          # Rotate every 1 day
        backupCount=7,        # Keep up to 7 log files (older files will be deleted)
        encoding="utf-8"     # Set encoding to UTF-8
    )

    # Set the logging format
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    # Set up the root logger with the handler
    logging.getLogger().setLevel(logging.INFO)  # or logging.DEBUG
    logging.getLogger().addHandler(handler)
    logging.getLogger('httpx').setLevel(logging.WARNING)

def create_config_file():
    # Default configuration content
    config_content = '''# config.py

# cookie
cookie = ''

# directory where logs will be saved
logs_dir = "logs"


# by creator: creator_id: str, count: int
# query type
query_type = "creator"  # Options: "creator", "detail"
# creator ids
creator_ids = [
    "MS4wLjABAAAAZJdMJWCk20BhmfjdkBtg_3OwU9tHA9aoKriwjS52wFo",  # Example: Creator ID for 哈工大
    # "......",                                                   # Add more creator IDs as needed
]
# number of videos to fetch for each creator
count = 2

# by detail: aweme_id: str
# aweme IDs to fetch details for specific videos
aweme_ids = [
    7411856833750519090,  # Example: Aweme ID for 哈工大军训又上新了
    # ......,               # Add more aweme IDs as needed
]
'''

    # Write the default configuration to config.py
    with open("config.py", "w", encoding="utf-8") as config_file:
        config_file.write(config_content)
    logging.info("config.py has been created and initialized with default values.")


def check_and_initialize_config():
    # Check if config.py exists
    if not os.path.exists("config.py"):
        logging.warning("config.py not found. Creating and initializing it...")
        create_config_file()
    else:
        logging.info("config.py found.")

# get aweme_ids by creator_id
async def get_creator_awesome_id(creator_id: str, count: int, cookie: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=600) as client:
        all_video_list = []
        max_cursor = ""
        has_more = True
        while has_more and len(all_video_list) < count:
            uri = "https://www.douyin.com/aweme/v1/web/aweme/post/"
            params = {
                "sec_user_id": creator_id,
                "count": count,
                "max_cursor": max_cursor,
                "locate_query": "false",
                "publish_video_strategy_type": 2,                                    # 暂时还不知道是什么
                'verifyFp': 'verify_m0tzzv90_eA8z0jDr_6N9p_4OBV_BdPt_lCYFfQxJlKKf',  # 确保替换为有效的值
                'fp': 'verify_m0tzzv90_eA8z0jDr_6N9p_4OBV_BdPt_lCYFfQxJlKKf'         # 确保替换为有效的值
            }
            headers = {"cookie": cookie}
            params, headers = common(uri, params, headers)
            response = await client.get(uri, params=params, headers=headers)
            response_data = response.json()
            
            aweme_list = response_data.get("aweme_list", [])
            all_video_list.extend(aweme_list)
            
            has_more = response_data.get("has_more", 0)
            max_cursor = response_data.get("max_cursor", "")
        
        # 由于可能获取到多的视频，这里进行处理
        video_infos = [
            {
                "aweme_id": video_item.get("aweme_id"),
                "desc": video_item.get("desc"),
                "create_time": video_item.get("create_time"),
                "nickname": video_item.get("author", {}).get("nickname", "")
            }
            for video_item in all_video_list[:count]
        ]

        return video_infos

# test
def get_creator_video_list_detail(creator_ids: list[str], count: int, cookie: str):
    # 测试获取用户的视频列表
    res = []
    for creator_id in creator_ids:
        res.extend(asyncio.run(get_creator_awesome_id(creator_id, count, cookie)))
    
    for i in res:
        print(f"{i['aweme_id']}: {i['desc']}; {i['create_time']}; {i['nickname']}")


async def get_comments_async(client: httpx.AsyncClient, aweme_id: str, cursor: str = "0", count: str = "50", cookie: str = '') -> dict[
    str, Any]:
    params = {"aweme_id": aweme_id, "cursor": cursor, "count": count, "item_type": 0}
    headers = {"cookie": cookie}
    params, headers = common(url, params, headers)
    response = await client.get(url, params=params, headers=headers)
    await asyncio.sleep(0.8)
    return response.json()


async def fetch_all_comments_async(aweme_id: str, cookie: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=600) as client:
        cookie = cookie
        cursor = 0
        all_comments = []
        has_more = 1
        with tqdm(desc="Fetching comments", unit="comment") as pbar:
            while has_more:
                response = await get_comments_async(client, aweme_id, cursor=str(cursor), cookie=cookie)
                comments = response.get("comments", [])
                if isinstance(comments, list):
                    all_comments.extend(comments)
                    pbar.update(len(comments))
                has_more = response.get("has_more", 0)
                if has_more:
                    cursor = response.get("cursor", 0)
                await asyncio.sleep(0.5)
        return all_comments


async def get_replies_async(client: httpx.AsyncClient, semaphore, comment_id: str, cursor: str = "0",
                            count: str = "50", cookie: str = '') -> dict:
    params = {"cursor": cursor, "count": count, "item_type": 0, "item_id": comment_id, "comment_id": comment_id}
    headers = {"cookie": cookie}
    params, headers = common(reply_url, params, headers)
    async with semaphore:
        response = await client.get(reply_url, params=params, headers=headers)
        await asyncio.sleep(0.5)  # 限制速度，避免请求过快
        return response.json()


async def fetch_replies_for_comment(client: httpx.AsyncClient, semaphore, comment: dict, pbar: tqdm, cookie: str) -> list:
    comment_id = comment["cid"]
    has_more = 1
    cursor = 0
    all_replies = []
    while has_more and comment["reply_comment_total"] > 0:
        response = await get_replies_async(client, semaphore, comment_id, cursor=str(cursor), cookie=cookie)
        replies = response.get("comments", [])
        if isinstance(replies, list):
            all_replies.extend(replies)
        has_more = response.get("has_more", 0)
        if has_more:
            cursor = response.get("cursor", 0)
        await asyncio.sleep(0.5)
    pbar.update(1)
    return all_replies


async def fetch_all_replies_async(comments: list, cookie: str) -> list:
    all_replies = []
    async with httpx.AsyncClient(timeout=600) as client:
        semaphore = asyncio.Semaphore(10)  # 在这里创建信号量
        with tqdm(total=len(comments), desc="Fetching replies", unit="comment") as pbar:
            tasks = [fetch_replies_for_comment(client, semaphore, comment, pbar, cookie) for comment in comments]
            results = await asyncio.gather(*tasks)
            for result in results:
                all_replies.extend(result)
    return all_replies


def process_comments(comments: list[dict[str, Any]]) -> pd.DataFrame:
    data = [{
        "评论ID": c['cid'],
        "评论内容": c['text'].replace("\n", "").replace("\r", ""),  # 去除换行符
        # "点赞数": c['digg_count'],
        "评论时间": datetime.fromtimestamp(c['create_time']).strftime('%Y-%m-%d %H:%M:%S'),
        "用户昵称": c['user']['nickname'],
        # "用户主页链接": f"https://www.douyin.com/user/{c['user']['sec_uid']}",
        # "用户抖音号": c['user']['unique_id'],
        # "用户签名": c['user']['signature'],
        # "回复总数": c['reply_comment_total'],
    } for c in comments]
    return pd.DataFrame(data)


def process_replies(replies: list[dict[str, Any]], comments: pd.DataFrame) -> pd.DataFrame:
    data = []
    for c in replies:
        try:
            # Find the matching row for the reply ID in comments
            matching_comments = comments.loc[comments['评论ID'] == c["reply_id"], '用户昵称']
            
            # Check if any matching comment exists
            if not matching_comments.empty:
                reply_to_user = matching_comments.iloc[0]  # Get the first matching user nickname
            else:
                reply_to_user = c["reply_to_username"]  # Default to reply_to_username if no match
            
            data.append({
                "评论ID": c["cid"],
                "评论内容": c["text"].replace("\n", "").replace("\r", ""),  # 去除换行符
                "评论时间": datetime.fromtimestamp(c["create_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                "用户昵称": c["user"]["nickname"],
                "回复的评论": c["reply_id"],
                "具体的回复对象": c["reply_to_reply_id"] if c["reply_to_reply_id"] != "0" else c["reply_id"],
                "回复给谁": reply_to_user
            })
        
        except Exception as e:
            # Log or print the error if any issue occurs during processing
            logging.error(f"Error processing reply with 评论ID {c['cid']}: {e}")
    
    return pd.DataFrame(data)


def save(data: pd.DataFrame, filename: str):
    data.to_csv(filename, index=False)


async def process_aweme_id(aweme_id, cookie):
    # 确保 'data' 文件夹存在
    if not os.path.exists("data"):
        os.makedirs("data")
    # 评论部分
    all_comments = await fetch_all_comments_async(aweme_id, cookie)
    logging.info(f"Found {len(all_comments)} comments for aweme_id {aweme_id}.")

    all_comments_ = process_comments(all_comments)
    comments_filename = f"data/{aweme_id}_comments.csv"
    save(all_comments_, comments_filename)

    # 回复部分 如果不需要直接注释掉
    all_replies = await fetch_all_replies_async(all_comments, cookie)
    logging.info(f"Found {len(all_replies)} replies for aweme_id {aweme_id}.")
    logging.info(f"Found {len(all_replies) + len(all_comments)} total for aweme_id {aweme_id}.")
    
    all_replies = process_replies(all_replies, all_comments_)
    replies_filename = f"data/{aweme_id}_replies.csv"
    save(all_replies, replies_filename)


def main():
    # Check and initialize config if needed
    check_and_initialize_config()
    # Import config after creating or confirming its existence
    import config

    setup_logging(config.logs_dir)
    logging.info("Logging has been set up.")

    try:
        if config.query_type == "detail":
            aweme_ids_main = config.aweme_ids
            for aweme_id in aweme_ids_main:
                asyncio.run(process_aweme_id(aweme_id, config.cookie))
        elif config.query_type == "creator":
            aweme_ids_main = []
            for creator_id in config.creator_ids:
                aweme_ids_main.extend(asyncio.run(get_creator_awesome_id(creator_id, config.count, config.cookie)))
            for video_info in aweme_ids_main:
                asyncio.run(process_aweme_id(video_info['aweme_id'], config.cookie))
                # print(video_info)
        else:
            logging.error(f"Invalid query_type: {config.query_type}")
            return
        db = crdb()
        db.process_data_folder()
        db.close()
        logging.info("Data has been successfully stored in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)  # Log the error and stack trace


# 运行 main 函数
if __name__ == "__main__":
    try:
        main()
        logging.info('Task all done!')
        # import config
        # get_creator_video_list_detail(config.creator_ids, config.count, config.cookie)
    except Exception as e:
        logging.error(f"An error occurred during the task: {e}", exc_info=True)
