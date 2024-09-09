import os
import sqlite3
import logging
import pandas as pd
from datetime import datetime

class crdb:
    def __init__(self):
        # Database connection
        self.db_path = "comments_replies.db"  # SQLite database file
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()  # Store cursor for later use

        # Create tables if they don't exist
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            评论ID TEXT PRIMARY KEY,
            评论内容 TEXT,
            评论时间 TEXT,
            用户昵称 TEXT,
            视频ID TEXT
        )
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS replies (
            评论ID TEXT PRIMARY KEY,
            评论内容 TEXT,
            评论时间 TEXT,
            用户昵称 TEXT,
            回复的评论 TEXT,
            具体的回复对象 TEXT,
            回复给谁 TEXT,
            视频ID TEXT
        )
        ''')

    # Function to process CSV files and handle errors
    def process_csv(self, file_path, table_name, video_id):
        try:
            data = pd.read_csv(file_path)
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            return  # Stop if there is an error processing the fileS
        
        data['视频ID'] = video_id  # Add video ID to each row
        
        # Remove any line breaks in the text fields
        for column in data.columns:
            if data[column].dtype == object:  # Only apply to text fields
                data[column] = data[column].str.replace(r'\n|\r', ' ', regex=True)
        
        # List to store successful entries
        successful_entries = []

        # Try inserting row by row to handle duplicates
        for index, row in data.iterrows():
            try:
                row.to_frame().T.to_sql(table_name, self.conn, if_exists='append', index=False)
                successful_entries.append(row)
            except sqlite3.IntegrityError as e:
                # Handle duplicate entry (Primary key violation) and skip
                logging.warning(f"Skipping duplicate 评论ID {row['评论ID']} in {table_name}: {e}")
        
        # If there are successful entries, save them to a separate CSV
        if successful_entries:
            success_df = pd.DataFrame(successful_entries)

            # Get current date and time (24-hour format)
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_time = datetime.now().strftime('%H')

            # Create folder path based on current date and time
            folder_path = f"data/{current_date}/{current_time}"
            os.makedirs(folder_path, exist_ok=True)  # Create directories if they don't exist

            # Save the successful entries to the new CSV in the formatted folder structure
            success_filename = f"{folder_path}/{video_id}_{table_name}.csv"
            # If the file already exists, read the existing data and append new entries
            if os.path.exists(success_filename):
                existing_data = pd.read_csv(success_filename)
                # Append new data and drop duplicates based on the primary key '评论ID'
                combined_data = pd.concat([existing_data, success_df]).drop_duplicates(subset=['评论ID'])
            else:
                # If the file doesn't exist, just use the new data
                combined_data = success_df
            
            # Save the combined data back to the same file
            combined_data.to_csv(success_filename, index=False)
            logging.info(f"Successful entries saved to {success_filename}")

    # Iterate over the 'data' folder and process each file
    def process_data_folder(self):
        data_folder = 'data'
        if not os.path.exists(data_folder):
            logging.warning(f"Folder {data_folder} does not exist!")
            return  # Stop if the folder doesn't exist

        for filename in os.listdir(data_folder):
            if filename.endswith('_comments.csv'):
                video_id = filename.split('_')[0]  # Extract video ID
                file_path = os.path.join(data_folder, filename)
                self.process_csv(file_path, 'comments', video_id)
            elif filename.endswith('_replies.csv'):
                video_id = filename.split('_')[0]  # Extract video ID
                file_path = os.path.join(data_folder, filename)
                self.process_csv(file_path, 'replies', video_id)

        # Commit changes to the database
        self.conn.commit()

    def close(self):
        # Close the connection separately, to be called when finished
        self.conn.close()
        logging.info("Database connection closed.")

# Usage Example
if __name__ == "__main__":
    db = crdb()
    db.process_data_folder()
    db.close()
    logging.info("Data has been successfully stored in the database.")
