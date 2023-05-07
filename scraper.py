import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import sys

BASE_URL = "https://www.pornhub.com"
headers = {'User-Agent': 'For educational purposes for Large-Scale data-processing practice. Please contact: bitsikokos@uchicago.edu'}
starting_url = "https://www.pornhub.com/video/random" #"https://www.pornhub.com/view_video.php?viewkey=ph5cdde27cdd47c"

def create_database_table():
    conn = sqlite3.connect('comments.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS comments (
                        username TEXT,
                        username_href TEXT,
                        video_url TEXT,
                        comment_text TEXT,
                        upvotes INTEGER)''')
    conn.commit()
    conn.close()

def insert_comment(username, username_href, video_url, comment_text, upvotes):
    conn = sqlite3.connect('comments.db')
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO comments 
           (username, username_href, video_url, comment_text, upvotes)
           VALUES (?, ?, ?, ?, ?)
        """, 
        (username, username_href, video_url, comment_text, upvotes)
        )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database_table()
    N = 5
    for i in range(N):
        response = requests.get(starting_url, headers=headers)
        video_url = response.url
        print(video_url)
        porn_soup = BeautifulSoup(response.text, "html.parser")

        user_data_blocks_list = porn_soup.findAll("div", {"class": "topCommentBlock clearfix"})
        comment_data_list= porn_soup.findAll("div", {"class": "commentMessage"})
        upvotes_data_list = porn_soup.findAll("div", {"class":"actionButtonsBlock"})
        # #Getting comments
        for user_block, comment_block, upvote_block \
            in zip(user_data_blocks_list[:-1],
                comment_data_list[:-1],
                upvotes_data_list[:-1]):
            if user_block.find("a"):
                user_href = user_block.find("a").get('href')
                user_name = user_block.find("a").find("img")['title']
                comment_text = comment_block.span.text
                upvote = upvote_block.find("span").text
                print((user_name, user_href, video_url, comment_text, upvote))
                insert_comment(user_name, user_href, video_url, comment_text, upvote)

    #     porn_comment_data = comment_block.findAll("span")
    #     if porn_comment_data:
    #         username_href = porn_comment_data[0].find('a')['href']
    #         username = porn_comment_data[0].find('a').text

    #         comment_text = porn_comment_data[1].text
    #         upvotes = porn_comment_data[2].text
    #         # insert_comment(username, username_href, video_url, comment_text, upvotes)
