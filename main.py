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
from config import *

url = "https://www.douyin.com/aweme/v1/web/comment/list/"
reply_url = url + "reply/"

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

async def get_comments_async(client: httpx.AsyncClient, aweme_id: str, cursor: str = "0", count: str = "50") -> dict[
    str, Any]:
    params = {"aweme_id": aweme_id, "cursor": cursor, "count": count, "item_type": 0}
    headers = {"cookie": cookie}
    params, headers = common(url, params, headers)
    response = await client.get(url, params=params, headers=headers)
    await asyncio.sleep(0.8)
    return response.json()


async def fetch_all_comments_async(aweme_id: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=600) as client:
        cursor = 0
        all_comments = []
        has_more = 1
        with tqdm(desc="Fetching comments", unit="comment") as pbar:
            while has_more:
                response = await get_comments_async(client, aweme_id, cursor=str(cursor))
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
                            count: str = "50") -> dict:
    params = {"cursor": cursor, "count": count, "item_type": 0, "item_id": comment_id, "comment_id": comment_id}
    headers = {"cookie": cookie}
    params, headers = common(reply_url, params, headers)
    async with semaphore:
        response = await client.get(reply_url, params=params, headers=headers)
        await asyncio.sleep(0.5)  # 限制速度，避免请求过快
        return response.json()


async def fetch_replies_for_comment(client: httpx.AsyncClient, semaphore, comment: dict, pbar: tqdm) -> list:
    comment_id = comment["cid"]
    has_more = 1
    cursor = 0
    all_replies = []
    while has_more and comment["reply_comment_total"] > 0:
        response = await get_replies_async(client, semaphore, comment_id, cursor=str(cursor))
        replies = response.get("comments", [])
        if isinstance(replies, list):
            all_replies.extend(replies)
        has_more = response.get("has_more", 0)
        if has_more:
            cursor = response.get("cursor", 0)
        await asyncio.sleep(0.5)
    pbar.update(1)
    return all_replies


async def fetch_all_replies_async(comments: list) -> list:
    all_replies = []
    async with httpx.AsyncClient(timeout=600) as client:
        semaphore = asyncio.Semaphore(10)  # 在这里创建信号量
        with tqdm(total=len(comments), desc="Fetching replies", unit="comment") as pbar:
            tasks = [fetch_replies_for_comment(client, semaphore, comment, pbar) for comment in comments]
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


async def process_aweme_id(aweme_id):
    # 确保 'data' 文件夹存在
    if not os.path.exists("data"):
        os.makedirs("data")
    # 评论部分
    all_comments = await fetch_all_comments_async(aweme_id)
    logging.info(f"Found {len(all_comments)} comments for aweme_id {aweme_id}.")

    all_comments_ = process_comments(all_comments)
    comments_filename = f"data/{aweme_id}_comments.csv"
    save(all_comments_, comments_filename)

    # 回复部分 如果不需要直接注释掉
    all_replies = await fetch_all_replies_async(all_comments)
    logging.info(f"Found {len(all_replies)} replies for aweme_id {aweme_id}.")
    logging.info(f"Found {len(all_replies) + len(all_comments)} total for aweme_id {aweme_id}.")
    
    all_replies = process_replies(all_replies, all_comments_)
    replies_filename = f"data/{aweme_id}_replies.csv"
    save(all_replies, replies_filename)


def main():
    try:
        for aweme_id in aweme_ids:
            asyncio.run(process_aweme_id(aweme_id))
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
    except Exception as e:
        logging.error(f"An error occurred during the task: {e}", exc_info=True)
