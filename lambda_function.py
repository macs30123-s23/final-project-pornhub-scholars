
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


def upsert_comment(username_href, view_key, comment_text, upvotes, timestamp):
    data = dict(username_href=username_href, view_key=view_key, comment_text=comment_text, upvotes=upvotes, timestamp=timestamp)
    comments_table.upsert(data, ['username_href', 'view_key', 'comment_text'])


def upsert_video(
    view_key,
    title,
    creator_name,
    creator_href,
    views,
    rating,
    year_added,
    categories,
    timestamp,
):
    data = dict(view_key=view_key, title=title, creator_name=creator_name, creator_href=creator_href, views=views, rating=rating, year_added=year_added, categories=categories, timestamp=timestamp)
    video_info_table.upsert(data, ['view_key'])


def upsert_creator(
    creator_href,
    creator_name,
    creator_type,
    about_info,
    video_count,
    subscribers,
    infos,
    timestamp,
):
    data = dict(creator_href=creator_href, creator_name=creator_name, creator_type=creator_type, about_info=about_info, video_count=video_count, subscribers=subscribers, infos=str(infos), timestamp=timestamp)
    creators_table.upsert(data, ['creator_href'])


def scrape_and_insert_comments(porn_soup, view_key):
    user_data_blocks_list = porn_soup.findAll(
        "div", {"class": "topCommentBlock clearfix"}
    )
    comment_data_list = porn_soup.findAll("div", {"class": "commentMessage"})
    upvotes_data_list = porn_soup.findAll("div", {"class": "actionButtonsBlock"})
    for user_block, comment_block, upvote_block in zip(
        user_data_blocks_list[:-1], comment_data_list[:-1], upvotes_data_list[:-1]
    ):
        if user_block.find("a"):
            user_href = user_block.find("a").get("href")
            comment_text = comment_block.span.text
            upvote = int(upvote_block.find("span").text)
            timestamp = time.time()
            upsert_comment(user_href, view_key, comment_text, upvote, timestamp)

def scrape_and_insert_video_and_creator(porn_soup, view_key):
    # scrape insert video info
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
            creator_name = porn_soup.find("a", {"class": "gtm-event-link bolded"}).text
        except AttributeError:
            creator_name = None
        try:
            creator_href = porn_soup.find("a", {"class": "gtm-event-link bolded"})[
                "href"
            ]
        except AttributeError:
            creator_href = None
    else:
        try:
            creator_name = (
                porn_soup.find("div", {"class": "userInfoContainer"}).find("a").text
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

    try:
        creator_type = creator_href.split("/")[1]
    except AttributeError:
        creator_type = None

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

    if creator_href:
        model_response = requests.get(BASE_URL + creator_href, headers=headers)
    else:
        return

    model_soup = BeautifulSoup(model_response.text, "html.parser")
    infos = {}
    try:
        about_info = model_soup.find("div", {"class": "about"}).find("div").text.strip()
    except AttributeError:
        about_info = None

    if creator_type == "model":
        for info_tag in model_soup.findAll("div", {"class": "infoPiece"}):
            key = info_tag.find("span").text.strip().rstrip(":")
            try:
                value = info_tag.find("span", {"class": "smallInfo"}).text.strip()
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


def lambda_handler(event, context):
    """
    Scrape book info from a list of urls and store in database
    """

    # Scraping elements
    BASE_URL = "https://www.pornhub.com"
    headers = {
        "User-Agent": "For educational purposes for Large-Scale data-processing practice. Please contact: bitsikokos@uchicago.edu"
    }
    starting_url = "https://www.pornhub.com/video/random"

    # Use global keyword to reference the global variables
    global db_url
    global db
    global comments_table
    global video_info_table
    global creators_table

    # database elements
    db_url = event["db_url"]
    db = dataset.connect(db_url)
    comments_table = db['comments']
    video_info_table = db['video_info']
    creators_table = db['creators']

    # how many videos to scrape
    N = event["N"]

    start_time = time.time()

    for i in range(N):
        response = requests.get(starting_url, headers=headers)
        # TODO: if a video is already scraped, double entries appear in the comments table
        video_url = response.url
        print(video_url)
        # TODO: this was implemented assuming that all links are in the form of
        #       https://www.pornhub.com/view_video.php?viewkey=928509562
        #       however, there are links (rarely) that are in the form:
        #       https://www.modelhub.com/video/5e41c74eb8c92
        # which means that video_url shouold be also included in the table
        # for now (with the try and except) we are ignoring the modelhub links
        try:
            view_key = re.findall(r"viewkey=([a-zA-Z0-9]+)", video_url)[0]
        except IndexError:
            continue
        porn_soup = BeautifulSoup(response.text, "html.parser")

        scrape_and_insert_video_and_creator(porn_soup, view_key)
        scrape_and_insert_comments(porn_soup, view_key)
    end_time = time.time()
    print(f"Elapsed time: {end_time-start_time} seconds")
    return {
        "statusCode": 200,
        "body": json.dumps(f"Elapsed time: {end_time-start_time} seconds"),
    }