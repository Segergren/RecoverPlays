from typing import cast
from bs4 import BeautifulSoup
import requests
import urllib.request
import re
import mysql.connector
from threading import Thread
import threading
import time
import os
import ffmpeg
import sys
import shutil
from datetime import datetime
import re
import logging
import random
import html

class Resolution:
  def __init__(self, width, height):
    self.width = width
    self.height = height

class Video:
  def __init__(self, url, title, game, views, date):
    self.url = url
    self.title = title
    self.game = game
    self.views = views
    self.date = date


videos = []
last_video_checked_id = ""
plays_videos_page_count = "1"
plays_user_id = ""
videos_downloaded_count = 0
username = html.escape(str(sys.argv[1]))
resolution_list = [Resolution(1280, 720), Resolution(960, 540), Resolution(896, 504), Resolution(864, 486), Resolution(640, 360), Resolution(320, 180)]
latest_429 = datetime(1970, 1, 1)

#Creating a logfile
current_time = datetime.now().strftime("%H-%M-%S")
logg_file = "C:/Users/database/Desktop/PlaysRecover/plays-loggs/" + username + "/" + current_time + ".log"
if not os.path.exists("C:/Users/database/Desktop/PlaysRecover/plays-loggs/" + username + "/"):
    os.makedirs("C:/Users/database/Desktop/PlaysRecover/plays-loggs/" + username + "/")
logging.basicConfig(filename=logg_file, encoding='utf-8', level=logging.DEBUG)

#Generate a thumbnail.
def generate_thumbnail(in_filename, out_filename):
    probe = ffmpeg.probe(in_filename)
    time = float(probe['streams'][0]['duration']) // 2
    
    for resolution in resolution_list:
        try:
            (
                ffmpeg
                .input(in_filename, ss=time)
                .filter('crop', resolution.width, resolution.height)
                .output(out_filename, vframes=1)
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )

            #If success, break out from the loop.
            break
        except Exception:
            pass

#Function for handling a video (Could be improved to support Single-responsibility principle)
def handle_video(video_element):
    global videos_downloaded_count
    global latest_429

    #Video title
    title = video_element.find_all("a",{"class","title"})[0].text

    #Get the video ID
    video_id = video_element.find_all("a",{"class","title"})[0]['href'].replace("/video/","").split("/")[0]
    if(len(video_id) < 1):
        video_id = video_element.find_all("a",{"class","title"})[0]['href'].replace("/video/","").split("/")[5].replace("plays.tv","")

    #Get the actual URL to search Wayback machine with. 
    archive_url = video_element.find_all("source")[0]['src'].replace("//","https://").replace("preview_144.mp4","*")
    
    max_tries = 30
    while(max_tries >= 0):
        try:

            #Checks if the Wayback machine has returned 429 in the latest 10 seconds
            time_difference = (datetime.now() - latest_429).total_seconds()
            if(time_difference < 10):
                print("Sleeping")
                time.sleep(random.randint(15,25))
            else:
                #Get the archived results
                archive_url = "https://archive.org/wayback/available?url=" + "https://plays.tv/video/" + video_id + "*"
                print("Trying to get: " + archive_url)
                archive_req = requests.get(archive_url, allow_redirects=False)
                print("Returned: " + str(archive_req.text))
                archive_data = archive_req.json()

                #If the video might have been saved
                if(len(archive_data["archived_snapshots"]) > 0):
                    print("Found: " + str(archive_data["archived_snapshots"]))
                    download_video(archive_data["archived_snapshots"]["closest"]["url"], video_id, title)
                
                #Count done videos (will count after the video was downloaded)
                videos_downloaded_count += 1
                print("Done: " + str(videos_downloaded_count) + " | " + "All: " + str(len(threads)-1))
                return 
        except Exception as ex:
            if('Max retries' in str(ex) or 'target machine actively refused it' in str(ex) or 'host has failed to respond' in str(ex)):
                latest_429 = datetime.now()
                print("Max tries.")
                time.sleep(random.randint(50,70))
            elif('list index out of range' in str(ex)):
                logging.warning(str(ex))
                videos_downloaded_count += 1
                return
            else:
                latest_429 = datetime.now()
                print("Unknown error: " + str(ex) + " | Try: " + str(max_tries))
                print("---------------")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print(archive_url)
                print("---------------")
                logging.warning(str(ex))
                time.sleep(random.randint(15,25))
                max_tries = max_tries - 1
    videos_downloaded_count += 1
    database = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="Plays-tv"
    )

    #Update downloaded status
    database_cursor = database.cursor()
    sql = "UPDATE fetching SET status = '" + str(videos_downloaded_count) + "/" + str(len(threads)-1) + "' WHERE LOWER(username)=%s"
    val = (username,)
    database_cursor.execute(sql, val)
    database.commit()
    database_cursor.close()
    database.close()
    print("Done: " + str(videos_downloaded_count) + " | " + "All: " + str(len(threads)-1))

def error_exit(ex):
    print("Error, exiting." + str(ex))
    logging.error(str(ex))
    database = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="Plays-tv"
    )

    #Set fetching to 0 (Done)
    database_cursor = database.cursor()
    sql = "UPDATE fetching SET currently_fetching = 0 WHERE LOWER(username)=%s"
    val = (username,)
    database_cursor.execute(sql, val)
    database.commit()

    #Delete all files
    if os.path.exists(video_path):
        shutil.rmtree(video_path)
    if os.path.exists(thumbnail_path):
        shutil.rmtree(thumbnail_path)
    exit()

#A fix for fixing dates for a small number of videos.
#Some videos on Plays.tv returned date as (Monday, Tuesday...). So we need
#to convert that date to real dates.
def date_fix(date):
    try:
        if(len(date.split()) == 1):
            match date:
                case 'Monday':
                    return "Dec 9 2019"
                case 'Tuesday':
                    return "Dec 3 2019"
                case 'Wednesday':
                    return "Dec 4 2019"
                case 'Thursday':
                    return "Dec 5 2019"
                case 'Friday':
                    return "Dec 6 2019"
                case 'Saturday':
                    return "Dec 7 2019"
                case 'Sunday':
                    return "Dec 8 2019"
        elif(len(date.split()) == 2):
            return date + " 2019"
        else:
            return date
    except Exception as ex:
        logging.error(str(ex))
        return date



def download_video(url, data_feed_id, title):
    global videos_downloaded_count
    global latest_429

    current_video = Video(url,title,None,None,None)

    video_source_tag = None
    video_soup = None

    #Download the video PAGE
    video_req = requests.get(current_video.url, allow_redirects=False)
    video_soup = BeautifulSoup(video_req.content, 'html.parser')
    video_source_tag = video_soup.find_all("source")

    #Get game, views and date
    try:
        current_video.game = (video_soup.find_all("a", {"class": "game-link"})[0].text)
    except Exception as ex:
        pass
    try:
        current_video.views = int(str(video_soup.find_all("span", {"class": "views-text"})[0].text).split()[0])
    except Exception as ex:
        pass
    try:
        current_video.date = str(datetime.strptime(date_fix(str(video_soup.find_all("a",{"class","created-time"})[0].text)), '%b %d %Y').date())
    except Exception as ex:
        pass

    #Different Plays video qualities
    qualities = [720,480,1080]

    #The base URL (without quality)
    video_base_url = ""

    for source in video_source_tag:
        quality = str(source['src'].split("/")[-1]).replace(".mp4","")
        temp_video_url = str(source['src']).replace(quality + ".mp4","").replace("//web","https://web")

        if('transcoded' in temp_video_url and 'preview' not in quality):
            video_base_url = temp_video_url
        if('processed' in temp_video_url and 'preview' not in quality):
            video_base_url = temp_video_url
        if('preview' not in quality):
            try:
                qualities.append(int(quality))
            except:
                print("Error quality")
    qualities = list(dict.fromkeys(qualities))

    #Try to download the video with the quality
    for quality in qualities:
        while(True):
            try:

                #Delay if the latest 429 was within 10 seconds.
                time_difference = (datetime.now() - latest_429).total_seconds()
                if(time_difference < 10):
                    time.sleep(random.randint(15,25))
                else:
                    #Download the video
                    download_url = video_base_url + str(quality) + ".mp4"
                    urllib.request.urlretrieve(download_url, video_path +"/"+data_feed_id + ".mp4")
                    generate_thumbnail(video_path +"/"+data_feed_id + ".mp4", thumbnail_path +"/"+data_feed_id + ".jpg")
                    database = mysql.connector.connect(
                        host="localhost",
                        user="root",
                        password="",
                        database="Plays-tv"
                    )
                    databaseCursor = database.cursor()
                    try:
                        #Insert the video metadata
                        sql = "INSERT INTO plays (id, username, video_id, video_name, video_date, views, game) VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                        val = (username, str(data_feed_id), str(current_video.title), str(current_video.date), current_video.views, current_video.game)
                        databaseCursor.execute(sql, val)
                        database.commit()
                    except Exception as ex:
                        print("Error: " + str(ex))
                        print("Trying next method...")
                        try:
                            #Removing character from title and game (might be the cause of the error)
                            current_video.title = re.sub(r"[^a-zA-Z0-9]+", ' ', current_video.title).strip()
                            current_video.game = re.sub(r"[^a-zA-Z0-9]+", ' ', current_video.game).strip()
                            
                            #Insert the video metadata
                            sql = "INSERT INTO plays (id, username, video_id, video_name, video_date, views, game) VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                            val = (username, str(data_feed_id), str(current_video.title), str(current_video.date), current_video.views, current_video.game)
                            databaseCursor.execute(sql, val)
                            database.commit()
                            print("Worked!")
                        except Exception as ex:
                            print("")
                            logging.error(str(ex))
                            print("Error again...")
                    time.sleep(1)
                    print("INSERT DONE")

                    #Update video count
                    sql = "UPDATE fetching SET status = '" + str(videos_downloaded_count) + "/" + str(len(threads)-1) + "' WHERE LOWER(username)=%s"
                    val = (username,)
                    databaseCursor.execute(sql, val)
                    database.commit()
                    databaseCursor.close()
                    database.close()
                    return
            except Exception as ex:
                if('Max retries' in str(ex) or 'target machine actively refused it' in str(ex) or 'host has failed to respond' in str(ex)):
                    print(str(ex))
                    latest_429 = datetime.now()
                    time.sleep(30)
                elif('503 Service Unavailable' in str(ex)):
                    print(str(ex))
                    time.sleep(10)
                elif('[Errno 11001]' in str(ex)):
                    print("BIG ERROR: " + str(download_url))
                    break
                else:        
                    print("Quality does not exist: " + str(ex))
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print(download_url)
                    print(video_path +"/"+data_feed_id + ".mp4")
                    #The quality does not exist, trying a worse quality.
                    break
#--------------------------------------------------------------------------
#This is the start of the program
video_path = 'C:/Users/database/Desktop/PlaysRecover/videos/' + username
thumbnail_path = 'C:/Users/database/Desktop/PlaysRecover/thumbnail/' + username

#Delete old stuff from the user if it has already fetched once (aka Refetch)
try:
    database = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="Plays-tv"
    )
    databaseCursor = database.cursor()
    sql = "DELETE FROM plays WHERE LOWER(username)=%s"
    val = (username.lower(),)
    databaseCursor.execute(sql, val)
    database.commit()
    sql = "DELETE FROM fetching WHERE LOWER(username)=%s"
    val = (username.lower(),)
    databaseCursor.execute(sql, val)
    database.commit()
    sql = "INSERT INTO fetching (username, currently_fetching) VALUES (%s,1)"
    val = (username,)
    databaseCursor.execute(sql, val)
    database.commit()
    databaseCursor.close()
    database.close()

    print("SQL INSERTED")
except Exception as ex:
    print("SQL error: " + str(ex))
    logging.error(str(ex))

if os.path.exists(video_path):
    shutil.rmtree(video_path)
if os.path.exists(thumbnail_path):
    shutil.rmtree(thumbnail_path)

#Create video and thumbnail directory
os.mkdir(video_path)
os.mkdir(thumbnail_path)

while(True):
    try:
        #Get the users plays profile
        profile_archive_url = "https://archive.org/wayback/available?url=" + "https://plays.tv/u/" + username
        profile_archive_req = requests.get(profile_archive_url, allow_redirects=False)
        profile_archive_data = profile_archive_req.json()
        print(profile_archive_data)

        #Check if the profile was archived
        if(len(profile_archive_data["archived_snapshots"]) > 0):
            profile_url = profile_archive_data["archived_snapshots"]["closest"]["url"]
            tries = 0
            while(tries < 10):
                profile_req = requests.get(profile_url, allow_redirects=False)
                profile_soup = BeautifulSoup(profile_req.content, 'html.parser')
                if('503 Service Unavailable' not in str(profile_soup)):
                    print("503, retrying")
                    tries = tries + 1
                    time.sleep(15)
                    break

            #Get user ID
            plays_user_id = profile_soup.find_all("button",{'class','btn-follow'})[0]['data-obj-id']
            profile_picture = profile_soup.find_all("img",{'class','profile-avatar'})[0]['data-lazyload'].replace("//web","https://web")
            
            #Get user Profile picture
            profile_picture_dates = str(profile_picture).split("/")[4]
            profile_picture = profile_picture.replace(profile_picture_dates,profile_picture_dates + "if_")
            urllib.request.urlretrieve(profile_picture, thumbnail_path +"/logo.jpg")
        break
    except Exception as ex:
        print(str(ex))
        if('Max retries' in str(ex) or 'target machine actively refused it' in str(ex) or 'host has failed to respond' in str(ex)):
            time.sleep(30)
        elif('Expecting value' in str(ex)):
            database = mysql.connector.connect(
                host="localhost",
                user="root",
                password="",
                database="Plays-tv"
            )
            databaseCursor = database.cursor()
            sql = "UPDATE fetching SET currently_fetching = -2 WHERE LOWER(username)=%s"
            val = (username,)
            databaseCursor.execute(sql, val)
            database.commit()
            databaseCursor.close()
            database.close()
            time.sleep(120)
            
        else:
            logging.warning(str(ex))
            break
print("USER ID: " +plays_user_id)

#Quick fix for removing some useless duplicated videos
try:
    for div in profile_soup.find_all("div", {'class':'mod-user-activity'}): 
        div.decompose()
except Exception as ex:
    error_exit(ex)

threads = []

#Gets the first videos (Needs to be implemented this way because of lazyloading)
try:
    start_videos = profile_soup.find_all("li",{"class","video-item"})
except Exception as ex:
    error_exit(ex)

for start_video in start_videos:
    try:
        thread = threading.Thread(target=handle_video, args=(start_video,))
        threads.append(thread)
    except Exception as ex:
        logging.warning(str(ex))

try:
    last_video_checked_id = start_videos[-1].find_all("a",{"class","title"})[0]['href'].replace("/video/","").split("/")[5].replace("plays.tv","")
except Exception as ex:
    error_exit(ex)

tries = 0
while(True):
    #Gets all pages of videos
    url = "https://web.archive.org/web/20191210164533/https://plays.tv/ws/module?section=videos&page_num=" + plays_videos_page_count + "&target_user_id=" + plays_user_id + "&infinite_scroll=true&last_id=" + last_video_checked_id + "&custom_loading_module_state=appending&infinite_scroll_fire_only=true&format=application%2Fjson&id=UserVideosMod"
    print("Loading new page: " + url)
    try:
        req = requests.get(url, allow_redirects=True)
        data = req.json()["body"]
        if(data == ""):
            break
        soup = BeautifulSoup(data, 'html.parser')
        source_tag = soup.find_all("li",{"class","video-item"})
        last_video_checked_id = source_tag[-1].find_all("a",{"class","title"})[0]['href'].replace("/video/","").split("/")[0]

        for video_element in source_tag:
            #handle_video(video_element)
            thread = threading.Thread(target=handle_video, args=(video_element,))
            threads.append(thread)
        plays_videos_page_count = str(int(plays_videos_page_count)+1)
        database = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="Plays-tv"
        )
        databaseCursor = database.cursor()
        sql = "UPDATE fetching SET status = '" + str(videos_downloaded_count) + "/" + str(len(threads)-1) + "' WHERE LOWER(username)=%s"
        val = (username,)
        databaseCursor.execute(sql, val)
        database.commit()
        databaseCursor.close()
        database.close()
    except Exception as ex:
        print(str(ex))
        if('Max retries' in str(ex) or 'target machine actively refused it' in str(ex) or 'host has failed to respond' in str(ex)):
            time.sleep(30)
        elif('503 Service Unavailable' in str(ex)):
            print("503, retrying")
            tries = tries + 1
            if(tries == 10):
                break
            time.sleep(15)
        else:
            logging.warning(str(ex))
            break

# Start all threads (downloads)
thread_count = 0
for x in threads:
    print(str(thread_count) + ": Starting a new thread")
    x.start()
    thread_count = thread_count + 1
# Wait for all threads to finish
for x in threads:
    print("Wait for finish")
    x.join()

logging.info("Done... Inserting into the database.")
database = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="Plays-tv"
)
databaseCursor = database.cursor()
sql = "UPDATE fetching SET currently_fetching = 0 WHERE LOWER(username)=%s"
val = (username.lower(),)
databaseCursor.execute(sql,val)
database.commit()
databaseCursor.close()
database.close()
logging.info("Inserting into the database. Done!")
