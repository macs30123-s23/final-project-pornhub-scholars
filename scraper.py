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
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS video_info (
                        title TEXT,
                        creator_name TEXT,
                        creator_href TEXT,
                        video_url TEXT,
                        views TEXT, 
                        rating TEXT, 
                        year_added TEXT,
                        categories TEXT
                        )""")
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

def insert_video(title, creator_name, creator_href, url, views, rating, year_added, categories):
    conn = sqlite3.connect('comments.db')
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO video_info 
           (title, creator_name, creator_href, video_url, views, rating, year_added, categories)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        (title, creator_name, creator_href, url,views, rating, year_added, categories)
        )
    conn.commit()
    conn.close()

def insert_creator():
    pass

def scrape_and_insert_comments(porn_soup, video_url):
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
            user_name = user_block.find("a").find("img").get('title')
            comment_text = comment_block.span.text
            upvote = upvote_block.find("span").text
            insert_comment(user_name, user_href, video_url, comment_text, upvote)

def scrape_and_insert_video_info(porn_soup, video_url):
    video_title = porn_soup.find("span", {"class":"inlineFree"}).text
    video_views = porn_soup.find('li', {"class":"views"}).span.text
    video_rating_up = porn_soup.find('li', {"class":"rating up"}).span.text 
    video_added = porn_soup.find('li', {"class":"added"}).text
    video_categories = [i.text for i in porn_soup.findAll('span', {'class':'crowdTitle'})]

    # TODO: create profile table
    creator_video_count = porn_soup.find("div", {"class":"userInfoContainer"}).find("span",{"class":"videosCount"}).text
    subscribers_count = porn_soup.find("div", {"class":"userInfoContainer"}).find("span",{"class":"subscribersCount"}).text

    if porn_soup.find("a", {"class":"gtm-event-link bolded"}):
        creator_name =  porn_soup.find("a", {"class":"gtm-event-link bolded"}).text
        creator_href =  porn_soup.find("a", {"class":"gtm-event-link bolded"})['href']
    else:
        creator_name = porn_soup.find("div", {"class":"userInfoContainer"}).find("a").text
        creator_href = porn_soup.find("div", {"class":"userInfoContainer"}).find("a").get('href')
    insert_video(video_title, creator_name, creator_href,video_url, video_views, video_rating_up, video_added, str(video_categories))

if __name__ == "__main__":
    create_database_table()
    N = 10
    for i in range(N):
        response = requests.get(starting_url, headers=headers)
        video_url = response.url
        print(video_url)
        porn_soup = BeautifulSoup(response.text, "html.parser")

        scrape_and_insert_video_info(porn_soup, video_url)
        scrape_and_insert_comments(porn_soup, video_url)
