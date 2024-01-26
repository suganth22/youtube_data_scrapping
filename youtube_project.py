# Importing the packages
import pandas as pd
import streamlit as st
import googleapiclient.discovery
from isodate import parse_duration
import pymongo
from pymongo.errors import DuplicateKeyError
import mysql.connector as mc
client = pymongo.MongoClient('mongodb://localhost:27017/')

# Credentials to fetch the data online
api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyC2z8MX0j9ylPhg3RhDVKS6gwE0LpyrMSQ'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Function to get the comment data of a video, by using video_id as input
def get_comment_data(video_id):
    request = youtube.commentThreads().list(part="snippet,replies",videoId=video_id,maxResults=5)
    response = request.execute()
    comment_data = []
    for i in range(len(response['items'])):
        value = {'comment_id':response['items'][i]['snippet']['topLevelComment']['id'],
                 'comment_author':response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                 'comment_text':response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay']}
        comment_data.append(value)
    return comment_data

# Function to get the video data of a playlist, by using playlist_id of a channel as input
def get_video_data(playlist_id):
    video_data = []
    next_page_token = None
    while True:
        request1 = youtube.playlistItems().list(part="snippet,contentDetails",playlistId=playlist_id,maxResults=50,pageToken = next_page_token)
        response1 = request1.execute()
        for i in range(len(response1['items'])):
            video_id = response1['items'][i]['contentDetails']['videoId']
            request2 = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id)
            response2 = request2.execute()
            if response2['items'][0]['contentDetails']['licensedContent'] == True:
                if 'commentCount' not in response2['items'][0]['statistics']:
                    commentCount = 0
                    comments = 'No comments'
                else:
                    commentCount = response2['items'][0]['statistics']['commentCount']
                    comments = get_comment_data(video_id)
                if 'likeCount' not in response2['items'][0]['statistics']:
                    likeCount = 0
                else:
                    likeCount = response2['items'][0]['statistics']['likeCount']
                if 'favoriteCount' not in response2['items'][0]['statistics']:
                    favoriteCount = 0
                else:
                    favoriteCount = response2['items'][0]['statistics']['favoriteCount']
                
                value = {'video_name':response1['items'][i]['snippet']['title'],
                     'video_id':video_id,
                     'video_desc':response1['items'][i]['snippet']['description'],
                     'video_pat':response1['items'][i]['snippet']['publishedAt'],
                     'comment_count':commentCount,
                     'favorite_count':response2['items'][0]['statistics']['favoriteCount'],
                     'like_count':likeCount,
                     'view_count':response2['items'][0]['statistics']['viewCount'],
                     'caption':response2['items'][0]['contentDetails']['caption'],
                     'duration':int(parse_duration(response2['items'][0]['contentDetails']['duration']).total_seconds()),
                     'thumbnail':response2['items'][0]['snippet']['thumbnails']['default']['url'],
                     'comments':comments }
                
                video_data.append(value)
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_data

# Function to get playlist data of a channel by taking channel id as input
def get_playlist_data(channel_id):
    playlist_data = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,maxResults=50,pageToken = next_page_token)
        response = request.execute()
        for i in range(len(response['items'])):
            playlist_id = response['items'][i]['id']
            value = {'playlist_name':response['items'][i]['snippet']['title'],
                     'playlist_id':response['items'][i]['id']}
            playlist_data.append(value)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data

# Function gets all required data using channel id as input, video data, comment data, playlist data functions are also called inside this function,
def get_channel_data(channel_id):
    channel_data = {}
    channel_ids = []
    db = client['youtube_data']
    coll = db['channel_data']
    for items in coll.find({},{'_id':0,'channel_id':1}):
        channel_ids.append(items['channel_id'])
    if channel_id not in channel_ids:
        request = youtube.channels().list(part="snippet,contentDetails,statistics,status",id=channel_id)
        response = request.execute()
        channel_data['channel_name'] = response['items'][0]['snippet']['title']
        channel_data['channel_id'] = response['items'][0]['id']
        channel_data['channel_desc'] = response['items'][0]['snippet']['description']
        channel_data['channel_status'] = response['items'][0]['status']['privacyStatus']
        channel_data['no_of_videos'] = response['items'][0]['statistics']['videoCount']
        channel_data['channel_views'] = response['items'][0]['statistics']['viewCount']
        channel_data['sub_count'] = response['items'][0]['statistics']['subscriberCount']
        channel_data['playlist_id'] = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channel_data['playlists_details'] = get_playlist_data(response['items'][0]['id'])
        channel_data['video_details'] = get_video_data(response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        coll.insert_one(channel_data)
        return 'Channel data scrapped and stored in mongodb'
    elif channel_id in channel_ids:
        return 'This channel data already exists'

# This Function is called to store the scrapped data into mongodb database(youtube_data), each channel data is stored as a collection within youtube_data database, and then transfered to SQL databases as tables
def data_to_sql():
    db = client['youtube_data']
    coll = db['channel_data']
    channel_data = []
    for items in coll.find({},{'_id':0,'video_details':0,'playlists_details':0,'channel_desc':0}):
        channel_data.append(items)
    video_data = []
    for items in coll.find({},{'_id':0,'channel_id':1,'video_details':1}):
        channel_id = items['channel_id']
        for i in items['video_details']:
            row = {'video_id':i['video_id'],
                   'video_name':i['video_name'],
                   'channel_id':channel_id,
                   'view_count':i['view_count'],
                   'comment_count':i['comment_count'],
                   'like_count':i['like_count'],
                   'duration':i['duration'],
                   'video_pat':i['video_pat'][:10] +' '+ i['video_pat'][11:19]}
            video_data.append(row)

    # establishing connection with SQL server
    mydb = mc.connect(host='localhost',user='root',password='suganth@2207')
    mycursor = mydb.cursor()
    mycursor.execute('show databases')
    dbs = []
    for i in mycursor:
        dbs.append(i)
    if ('youtube_data',) in dbs:
        mycursor.execute('drop database youtube_data')
    mycursor.execute('CREATE DATABASE youtube_data')
    mydb = mc.connect(host='localhost',user='root',password='suganth@2207', database = 'youtube_data')
    mycursor = mydb.cursor()

# Creation of tables Channel and video
    
    mycursor.execute("create table channel(channel_name varchar(255),channel_id varchar(255),channel_status varchar(255),no_of_videos int,channel_views bigint,sub_count bigint,playlist_id varchar(255))")
    mycursor.execute("create table video(video_id varchar(255),video_name varchar(255),channel_id varchar(255),view_count bigint,comment_count bigint,like_count bigint,duration int,video_pat datetime)")
    mydb.commit()

# inserting data in to the SQL tables   
    for i in channel_data:
        query = "INSERT INTO channel({}) VALUES ({})".format(', '.join(i.keys()),', '.join(['%s'] * len(i)))
        data = tuple(i.values())
        mycursor.execute(query,data)
        mydb.commit()

    for i in video_data:
        query = "INSERT INTO video({}) VALUES ({})".format(', '.join(i.keys()),', '.join(['%s'] * len(i)))
        data = tuple(i.values())
        mycursor.execute(query,data)
        mydb.commit()
    return 'Data retrieved from mongodb and moved to SQL tables successfully'

# Streamlit part
with st.sidebar:
    st.title(':Green[YouTube Data Harvesting and Warehousing]') 

channel_id = st.text_input('Enter the channel ID')
if st.button('Scrap data from youtube and store in mongodb'):
    st.success(get_channel_data(channel_id))

if st.button('Migrate data to SQL tables'):
    st.success(data_to_sql())

queries = st.selectbox('Select your question',('1.What are the names of all the videos and their corresponding channels?',
                                            '2.Which channels have the most number of videos, and how many videos do they have?',
                                            '3.What are the top 10 most viewed videos and their respective channels?',
                                            '4.How many comments were made on each video, and what are their corresponding video names?',
                                            '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                            '6.What is the total number of likes for each video, and what are their corresponding video names?',
                                            '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                            '8.What are the names of all the channels that have published videos in the year 2022?',
                                            '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                            '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))

mydb = mc.connect(host='localhost',user='root',password='suganth@2207', database = 'youtube_data')
mycursor = mydb.cursor()

if queries == '1.What are the names of all the videos and their corresponding channels?':
    query = '''SELECT video_name,channel_name FROM video,channel 
            where video.channel_id = channel.channel_id '''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['video_name','channel_name'])
    st.dataframe(df, height = 1000)

elif queries == '2.Which channels have the most number of videos, and how many videos do they have?':
    query = '''SELECT channel_name,no_of_videos FROM channel
            order by no_of_videos desc'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['channel_name','no_of_videos'])
    st.dataframe(df)

elif queries == '3.What are the top 10 most viewed videos and their respective channels?':
    query = '''SELECT video_name,view_count,channel_name FROM video,channel 
            where video.channel_id = channel.channel_id 
            ORDER BY view_count DESC limit 10'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['video_name','view_count','channel_name'])
    st.dataframe(df)

elif queries == '4.How many comments were made on each video, and what are their corresponding video names?':
    query = '''SELECT comment_count,video_name FROM video'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['comment_count','video_name'])
    st.dataframe(df)

elif queries == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query = '''SELECT video_name,like_count,channel_name FROM video,channel 
            where video.channel_id = channel.channel_id 
            order by like_count desc limit 100'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['video_name','like_count','channel_name'])
    st.dataframe(df)

elif queries == '6.What is the total number of likes for each video, and what are their corresponding video names?':
    query = '''SELECT video_name,like_count FROM video'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['video_name','like_count'])
    st.dataframe(df)

elif queries == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query = '''SELECT channel_name,channel_views FROM channel'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['channel_name','channel_views'])
    st.dataframe(df)

elif queries == '8.What are the names of all the channels that have published videos in the year 2022?':
    query = '''SELECT channel_name, count(*) as no_videos_2022
            FROM video,channel 
                where video.channel_id = channel.channel_id and 
                video_pat < '2022-12-31 23:59:59' and 
                video_pat > '2021-12-31 23:59:59'
                group by channel_name;'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['channel_name','no_videos_2022'])
    st.dataframe(df)

elif queries == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query = '''SELECT channel_name,avg(duration) as avg_duration
            FROM channel,video 
                where video.channel_id = channel.channel_id
                group by channel_name;'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['channel_name','avg_duration'])
    st.dataframe(df)

elif queries == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = '''SELECT channel_name, video_name,comment_count
            FROM channel,video 
                where video.channel_id = channel.channel_id
                order by comment_count desc limit 100'''
    mycursor.execute(query)
    table = mycursor.fetchall()
    df = pd.DataFrame(table, columns=['channel_name','video_name',''])
    st.dataframe(df)