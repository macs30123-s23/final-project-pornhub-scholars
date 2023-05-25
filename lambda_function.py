import requests
import dataset
import re
from datetime import datetime
from bs4 import BeautifulSoup
import json
import sys
import time
import os

# Global variables
db_url = None
db = None
comments_table = None
video_info_table = None
creators_table = None
BASE_URL = None
headers = None
starting_url = None


def upsert_comment(username_href, view_key, comment_text, upvotes, timestamp):
    """
    Insert or update comments table.

    Inputs:
        - username_href (str) commentator's username tag
        - view_key (str) video key
        - comment_text (str) the comment text
        - upvotes (int) number of upvotes for comment
        - timestamp (float) time

    Returns: None
    """
    data = dict(username_href=username_href, view_key=view_key,
                comment_text=comment_text, upvotes=upvotes, timestamp=timestamp)
    comments_table.upsert(data, ['username_href', 'view_key', 'comment_text'])


def upsert_video(view_key, title, creator_name, creator_href, views, rating,
                 year_added, categories, timestamp, ):
    """
    Insert or update the video_info table.

    Inputs:
        - view_key (str) the video key
        - title (str) title of the video
        - creator_name (str) the video's creator name
        - creator_href (str) the video's creator username tag
        - views (int) number of views the video has
        - rating (flaot) video's rating (from 0 to 1)
        - year_added (str) video added 'x years ago'
        - categories (str) a list of categories in a string format
        - timestamp (float) time

    Returns None
    """
    data = dict(view_key=view_key, title=title, creator_name=creator_name,
                creator_href=creator_href, views=views, rating=rating,
                year_added=year_added, categories=categories,
                timestamp=timestamp)
    video_info_table.upsert(data, ['view_key'])


def upsert_creator(creator_href, creator_name, creator_type, about_info,
                   video_count, subscribers, infos, timestamp, ):
    """
    Update or insert creators table

    Inputs:
        - creator_href (str) creator username tag
        - creator_name (str) name of the creator
        - creator_type (str) creator type --> 'channels', 'model', 'pornstar'
        - about_info (str) about info of creator
        - video_count (int) number of videos by creator
        - subscribers (int) number of subscribers
        - infos (str) creator info dictionary in a string format
        - timestamp (float) time
    """
    data = dict(creator_href=creator_href, creator_name=creator_name,
                creator_type=creator_type, about_info=about_info,
                video_count=video_count, subscribers=subscribers,
                infos=str(infos), timestamp=timestamp)
    creators_table.upsert(data, ['creator_href'])


def scrape_and_insert_comments(porn_soup, view_key):
    """
    Scrapes and inserts comments table

    Inputs:
        - porn_soup (bs4.BeautifulSoup) video page as bs4 soup object
        - view_key (str) the video's key tag

    Returns: None
    """
    user_data_blocks_list = porn_soup.findAll(
        "div", {"class": "topCommentBlock clearfix"}
    )
    comment_data_list = porn_soup.findAll("div", {"class": "commentMessage"})
    upvotes_data_list = porn_soup.findAll("div",
                                          {"class": "actionButtonsBlock"})
    for user_block, comment_block, upvote_block in zip(
            user_data_blocks_list[:-1], comment_data_list[:-1],
            upvotes_data_list[:-1]
    ):
        if user_block.find("a"):
            user_href = user_block.find("a").get("href")
            comment_text = comment_block.span.text
            upvote = int(upvote_block.find("span").text)
            timestamp = time.time()
            upsert_comment(user_href, view_key, comment_text, upvote, timestamp)


def scrape_and_insert_video_and_creator(porn_soup, view_key):
    """
    Scrapes and inserts video_info and creators tables

    Inputs:
        - porn_soup (bs4.BeautifulSoup) video page as bs4 soup object
        - view_key (str) the video's key tag

    Return: None
    """

    # scrape video info
    try:
        video_title = porn_soup.find("span", {"class": "inlineFree"}).text
    except AttributeError:
        video_title = None

    try:
        video_views = porn_soup.find("li", {"class": "views"}).span.text
        video_views = int(video_views.replace(",", ""))
    except AttributeError:
        video_views = None

    try:
        video_rating_up = porn_soup.find("li", {"class": "rating up"}).span.text
        video_rating_up = float(video_rating_up.replace("%", "")) / 100
    except AttributeError:
        video_rating_up = None

    try:
        video_added = porn_soup.find("li", {"class": "added"}).text
    except AttributeError:
        video_added = None

    try:
        video_categories = [
            i.text for i in porn_soup.findAll("span", {"class": "crowdTitle"})
        ]
    except AttributeError:
        video_categories = None

    if porn_soup.find("a", {"class": "gtm-event-link bolded"}):
        try:
            creator_name = porn_soup.find("a", {
                "class": "gtm-event-link bolded"}).text
        except AttributeError:
            creator_name = None
        try:
            creator_href = \
                porn_soup.find("a", {"class": "gtm-event-link bolded"})[
                    "href"
                ]
        except AttributeError:
            creator_href = None
    else:
        try:
            creator_name = (
                porn_soup.find("div", {"class": "userInfoContainer"}).find(
                    "a").text
            )
        except AttributeError:
            creator_name = None
        try:
            creator_href = (
                porn_soup.find("div", {"class": "userInfoContainer"})
                    .find("a")
                    .get("href")
            )
        except AttributeError:
            creator_href = None

    if creator_href:
        model_response = requests.get(BASE_URL + creator_href, headers=headers)
    else:
        try:
            creator_href = \
                porn_soup.find("div", {"class": "pornstarNameIcon"}).find("a")[
                    'href']
        except AttributeError:
            creator_href = None

        if creator_href:
            model_response = requests.get(BASE_URL + creator_href,
                                          headers=headers)
        else:
            print("Creator href error")
            return

    try:
        creator_type = creator_href.split("/")[1]
    except AttributeError:
        creator_type = None

    # scrape and insert creator info
    try:
        creator_video_count = (
            porn_soup.find("div", {"class": "userInfoContainer"})
                .find("span", {"class": "videosCount"})
                .text
        )
    except AttributeError:
        creator_video_count = None

    try:
        subscribers_count = (
            porn_soup.find("div", {"class": "userInfoContainer"})
                .find("span", {"class": "subscribersCount"})
                .text
        )
    except AttributeError:
        subscribers_count = None

    # scrape creator webpage
    model_soup = BeautifulSoup(model_response.text, "html.parser")
    infos = {}
    try:
        about_info = model_soup.find("div", {"class": "about"}).find(
            "div").text.strip()
    except AttributeError:
        about_info = None

    if creator_type == "model":
        for info_tag in model_soup.findAll("div", {"class": "infoPiece"}):
            key = info_tag.find("span").text.strip().rstrip(":")
            try:
                value = info_tag.find("span",
                                      {"class": "smallInfo"}).text.strip()
            except AttributeError:
                value = None
            infos[key] = value
    elif creator_type == "pornstar":
        for info_tag in model_soup.findAll("div", {"class": "infoBlock"}):
            key = info_tag.find("span").text.strip()
            value = info_tag.find("span", {"class": "smallInfo"}).text.strip()
            infos[key] = value
    else:  # channel
        pass

    # upsert tables
    timestamp = time.time()
    upsert_creator(
        creator_href,
        creator_name,
        creator_type,
        about_info,
        creator_video_count,
        subscribers_count,
        str(infos),
        timestamp,
    )

    timestamp = time.time()
    upsert_video(
        view_key,
        video_title,
        creator_name,
        creator_href,
        video_views,
        video_rating_up,
        video_added,
        str(video_categories),
        timestamp,
    )


def lambda_handler(event, context):
    """
    PornHub scraper lambda function.
    """
    # database elements
    db = None
    try:
        if 'Records' in event:
            record = event['Records'][0]

            data = json.loads(record['body'])

            # Use global keyword to reference the global variables
            global db_url
            global comments_table
            global video_info_table
            global creators_table
            global BASE_URL
            global headers
            global starting_url

            db_url = data["db_url"]
            db = dataset.connect(db_url)
            comments_table = db['comments']
            video_info_table = db['video_info']
            creators_table = db['creators']

            # Scraping elements
            BASE_URL = "https://www.pornhub.com"
            headers = {
                "User-Agent": """For educational purposes for Large-Scale
                                 data-processing practice.
                                 Please contact: bitsikokos@uchicago.edu
                              """
            }
            starting_url = "https://www.pornhub.com/video/random"

            # how many videos to scrape
            num_pages = data["num_pages"]

            start_time = time.time()

            for i in range(num_pages):
                response = requests.get(starting_url, headers=headers)
                video_url = response.url
                print(video_url)
                try:
                    view_key = re.findall(r"viewkey=([a-zA-Z0-9]+)",
                                          video_url)[0]
                except IndexError:
                    continue
                porn_soup = BeautifulSoup(response.text, "html.parser")

                scrape_and_insert_video_and_creator(porn_soup, view_key)
                scrape_and_insert_comments(porn_soup, view_key)
            end_time = time.time()
            print(f"Elapsed time: {end_time - start_time} seconds")

            return {
                "statusCode": 200,
                "body": json.dumps(
                    f"Elapsed time: {end_time - start_time} seconds"),
            }
    finally:
        if db:
            # close and dispose database connection
            db.engine.dispose()
            print("Database connection closed and disposed.")
