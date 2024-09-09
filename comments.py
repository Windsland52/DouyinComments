from db import *
import os
import sqlite3
import pandas as pd
from main import setup_logging
from config import logs_dir


class BadComment:
    def __init__(self):
        # Database connection
        self.db_path = "bad_comments.db"  # SQLite database file
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()  # Store cursor for later use

        # Create tables if they don't exist
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            评论ID TEXT PRIMARY KEY,
            评论内容 TEXT
        )
        ''')
    
    def add_comment(self, comment_id, comment_content):
        # Insert comment into database
        self.cursor.execute("INSERT INTO comments (评论ID, 评论内容) VALUES (?,?)", (comment_id, comment_content))
        self.conn.commit()
    
    # 导出所有数据到csv文件
    def data_to_csv(self):
        # Get all comments from database
        self.cursor.execute("SELECT * FROM comments")
        comments = self.cursor.fetchall()

        # Convert comments to pandas dataframe
        df = pd.DataFrame(comments, columns=['评论ID', '评论内容'])

        # Export dataframe to csv file
        df.to_csv('bad_comments.csv', index=False)

    def close(self):
        # Close database connection
        self.conn.close()


if __name__ == '__main__':
    # Test code
    setup_logging(logs_dir)
    bc = BadComment()
    cr = crdb()
    try:
        while True:
            comment_id = input("请输入评论ID：")
            comment_content = cr.get_comment_content(comment_id)
            bc.add_comment(comment_id, comment_content)
        bc.close()
        cr.close()
    except Exception as e:
        logging.error(e)