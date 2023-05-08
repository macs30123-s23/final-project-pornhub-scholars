import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import sys
import re

BASE_URL = "https://www.pornhub.com"
headers = {'User-Agent': 'For educational purposes for Large-Scale data-processing practice. Please contact: bitsikokos@uchicago.edu'}
starting_url = "https://www.pornhub.com/video/random" #"https://www.pornhub.com/view_video.php?viewkey=ph5cdde27cdd47c"
DB_NAME = 'comments3.db'
# TODO: find the set of keys 
MODEL_INFO = {'Gender': None,
 'Height': None,
 'Weight': None,
 'Hair Color': None,
 'Fake Boobs': None,
 'Tattoos': None,
 'Piercings': None,
 'Relationship status': None,
 'Interested in': None,
 'Interests and hobbie': None,
 'Turn Ons': None,
 'Profile Views': None,
 'Video Views': None,
 'Videos Watched': None,
 'Birth Place': None,
 'Ethnicity': None,
 'Turn Offs': None}

PORNSTAR_INFO = {'Gender': None,
 'Birth Place': None,
 'Star Sign': None,
 'Measurements': None,
 'Height': None,
 'Weight': None,
 'Ethnicity': None,
 'Background': None,
 'Hair Color': None,
 'Eye Color': None,
 'Fake Boobs': None,
 'Tattoos': None,
 'Piercings': None,
 'Interests and hobbies': None,
 'Relationship status': None,
 'Interested in': None,
 'Hometown': None,
 'City and Country': None,
 'Pornstar Profile Views': None,
 'Career Status': None,
 'Career Start and End': None,
 'Profile Views': None,
 'Videos Watched': None,
 'Video views': None}


def create_database_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS video_info (
                        view_key TEXT PRIMARY KEY,
                        title TEXT,
                        creator_name TEXT,
                        creator_href TEXT,
                        views TEXT, 
                        rating TEXT, 
                        year_added TEXT,
                        categories TEXT
                        )""")

    cursor.execute('''CREATE TABLE IF NOT EXISTS comments (
                        username_href TEXT,
                        view_key TEXT NOT NULL,
                        comment_text TEXT,
                        upvotes INTEGER,
                       FOREIGN KEY (view_key) REFERENCES video_info (view_key))''')
    
    conn.commit()
    conn.close()

def insert_comment(username_href, view_key, comment_text, upvotes):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO comments 
           (username_href, view_key, comment_text, upvotes)
           VALUES (?, ?, ?, ?)
        """, 
        (username_href, view_key, comment_text, upvotes)
        )
    conn.commit()
    conn.close()

def insert_video(view_key, title, creator_name, creator_href, views, rating, year_added, categories):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO video_info 
           (view_key, title, creator_name, creator_href, views, rating, year_added, categories)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        (view_key, title, creator_name, creator_href,views, rating, year_added, categories)
        )
    conn.commit()
    conn.close()

def insert_creator():
    pass

def scrape_and_insert_comments(porn_soup, view_key):
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
            # user_name = user_block.find("a").find("img").get('title')
            comment_text = comment_block.span.text
            upvote = upvote_block.find("span").text
            insert_comment(user_href, view_key, comment_text, upvote)

def scrape_and_insert_video_info(porn_soup, view_key):
    # scrape insert video info
    video_title = porn_soup.find("span", {"class":"inlineFree"}).text
    video_views = porn_soup.find('li', {"class":"views"}).span.text
    video_rating_up = porn_soup.find('li', {"class":"rating up"}).span.text 
    video_added = porn_soup.find('li', {"class":"added"}).text
    video_categories = [i.text for i in porn_soup.findAll('span', {'class':'crowdTitle'})]
    
    if porn_soup.find("a", {"class":"gtm-event-link bolded"}):
        creator_name =  porn_soup.find("a", {"class":"gtm-event-link bolded"}).text
        creator_href =  porn_soup.find("a", {"class":"gtm-event-link bolded"})['href']
    else:
        creator_name = porn_soup.find("div", {"class":"userInfoContainer"}).find("a").text
        creator_href = porn_soup.find("div", {"class":"userInfoContainer"}).find("a").get('href')
    creator_type = creator_href.split('/')[1]
    insert_video(view_key, video_title, creator_name, creator_href, video_views, video_rating_up, video_added, str(video_categories))

    # TODO: create creator profile table
    creator_video_count = porn_soup.find("div", {"class":"userInfoContainer"}).find("span",{"class":"videosCount"}).text
    subscribers_count = porn_soup.find("div", {"class":"userInfoContainer"}).find("span",{"class":"subscribersCount"}).text
    
    # TODO: video_title needs a try and except
    model_response = requests.get(BASE_URL+creator_href, headers=headers)
    model_soup = BeautifulSoup(model_response.text, "html.parser")
    if creator_type == 'model':
        about_info = model_soup.find("div", {"class":"about"}).find("div").text.strip()
        infos = {k:None for k in MODEL_INFO.keys()}
        for info_tag in model_soup.findAll("div", {"class":"infoPiece"}):
            key = info_tag.find('span').text.strip().rstrip(":")
            value = info_tag.find("span", {"class":"smallInfo"}).text.strip()    
            if key in infos:
                infos[key] = value
            else:
                print(f'Missing key: {key}')
    elif creator_type == 'pornstar':
        about_info = model_soup.find("div", {"class":"about"}).find("div").text.strip()
        # for stars
        infos = {k: None for k in PORNSTAR_INFO.keys()}
        for info_tag in model_soup.findAll("div", {"class":"infoBlock"}):
            key = info_tag.find('span').text.strip()
            values = info_tag.find("span", {"class":"smallInfo"}).text.strip()
            if key in infos:
                infos[key] = value
            else:
                print(f'Missing key: {key}')
            # sys.exit()
        # porn_soup.findAll("span", {"class":"smallInfo"})

if __name__ == "__main__":
    create_database_table()
    N = 100
    for i in range(N):
        response = requests.get(starting_url, headers=headers)
        video_url = response.url
        view_key = re.findall( r'viewkey=([a-zA-Z0-9]+)', video_url)[0]
        print(video_url)
        porn_soup = BeautifulSoup(response.text, "html.parser")

        scrape_and_insert_video_info(porn_soup, view_key)
        scrape_and_insert_comments(porn_soup, view_key)
