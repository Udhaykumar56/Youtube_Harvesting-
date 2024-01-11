from googleapiclient.discovery import build
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np

# _API _Key Connection 

def API_Connection():
    Api_Id="AIzaSyCBXKUOrHP0pGprrOWUXOrnCGzzKzmarPM"
    
    api_service_name = "youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube

youtube=API_Connection()

# _Getting_Channel_Info

def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )

    response=request.execute()

    for i in response['items']:
        data=dict(Channel_name=i["snippet"]["title"],
                  Channel_id=i["id"],
                  Subscriber=i["statistics"]["subscriberCount"],
                  Views=i["statistics"]["viewCount"],
                  Total_Videos=i["statistics"]["videoCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#_get_videos_ids

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part="contentDetails").execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextPageToken=None

    while True:
        response_playlist=youtube.playlistItems().list(
                                                    part='snippet',
                                                    playlistId=Playlist_Id,
                                                    maxResults=50,
                                                    pageToken=nextPageToken).execute()

        for i in range(len(response_playlist['items'])):
            video_ids.append(response_playlist['items'][i]['snippet']['resourceId']['videoId'])
        nextPageToken=response_playlist.get('nextPageToken')

        if nextPageToken is None:
            break
    return video_ids

#_Video_information

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)

        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_id=item['id'],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet']['description'],
                      Published_Date=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      Views=item['statistics'].get('viewCount'),
                      Likes=item['statistics'].get('likeCount'),
                      Comments=item['statistics'].get('commentCount'),
                      Favorite_Count=item['statistics']['favoriteCount'],
                      Definition=item['contentDetails']['definition'],
                      Caption_Status=item['contentDetails']['caption']
                     )
            video_data.append(data)
    return video_data

#_get_comment_info

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                          Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

#_get_playlist_info

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,ContentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_id=item['id'],
                      Title=item['snippet']['title'],
                      Channel_Id=item['snippet']['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      Published=item['snippet']['publishedAt'],
                      Video_count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token=response.get('nextpageToken')

        if next_page_token is None:
            break

    return All_data

#_upload_to_Mangodb

client=MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

#_Tabel_Creation_For_channel

def channels_table():
    mydb=psycopg2.connect(host="localhost",
                         user="postgres",
                         password="Born@1998",
                         database="youtube_data",
                         port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_name'],
                  row['Channel_id'],
                  row['Subscriber'],
                  row['Views'],
                  row['Total_Videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])


        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channel values are already inserted")

#_Tabel_Creation_For_Playlist

def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                         user="postgres",
                         password="Born@1998",
                         database="youtube_data",
                         port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                         Title varchar(100),
                                                         Channel_Id varchar(100),
                                                         Channel_Name varchar(100),
                                                         Published timestamp,
                                                         Video_count int)'''

    cursor.execute(create_query)
    mydb.commit()
    
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
            insert_query='''insert into playlists(Playlist_id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published,
                                                Video_count)

                                                values(%s,%s,%s,%s,%s,%s)'''

            values = (row['Playlist_id'],
                      row['Title'],
                      row['Channel_Id'],
                      row['Channel_Name'],
                      row['Published'],
                      row['Video_count'])



            cursor.execute(insert_query,values)
            mydb.commit()

#_Tabel_Creation_For_videos

def videos_table():
    mydb=psycopg2.connect(host="localhost",
                         user="postgres",
                         password="Born@1998",
                         database="youtube_data",
                         port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_name varchar(100),
                                                      Channel_Id varchar(100),
                                                      Video_id varchar(30) primary key,
                                                      Title varchar(100),
                                                      Tags text,
                                                      Thumbnail varchar(200),
                                                      Description text,
                                                      Published_Date timestamp,
                                                      Duration interval,
                                                      Views bigint,
                                                      Likes bigint,
                                                      Comments int,
                                                      Favorite_Count int,
                                                      Definition varchar(10),
                                                      Caption_Status varchar(50))'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        insert_query='''insert into videos(Channel_name,
                                          Channel_Id,
                                          Video_id,
                                          Title,
                                          Tags,
                                          Thumbnail,
                                          Description,
                                          Published_Date,
                                          Duration,
                                          Views,
                                          Likes,
                                          Comments,
                                          Favorite_Count,
                                          Definition,
                                          Caption_Status)

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_name'],
                  row['Channel_Id'],
                  row['Video_id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status'])


        cursor.execute(insert_query,values)
        mydb.commit()

#_Tabel_Creation_For_comments

def comments_table():
    mydb=psycopg2.connect(host="localhost",
                         user="postgres",
                         password="Born@1998",
                         database="youtube_data",
                         port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(50),
                                                        Comment_Published timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index, row in df3.iterrows():
            insert_query='''insert into comments(Comment_id,
                                                 Video_Id,
                                                 Comment_Text,
                                                 Comment_Author,
                                                 Comment_Published)

                                            values(%s,%s,%s,%s,%s)'''

            values = (row['Comment_id'],
                      row['Video_Id'],
                      row['Comment_Text'],
                      row['Comment_Author'],
                      row['Comment_Published'])


            cursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables created Successfull "

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    
    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)
    
    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    
    return df3

#_streamlit_part

with st.sidebar:
    st.title(":Black[YOUTUBE DATA HAVERSTING AND WAREHOUSEING]")
    st.header(":green[Overview]")
    st.caption("The YouTube Data Harvesting and Warehousing Project is a comprehensive data-driven initiative aimed at extracting, processing, and storing valuable information from YouTube using Python, MongoDB, and SQL. This project combines the power of Python for data extraction, MongoDB for flexible and scalable NoSQL storage, and SQL for structured data analysis and reporting.")
    st.header(":green[Key Points]")
    st.header("Data Harvesting with Python:")
    st.caption("Harvested data includes video metadata, comments, likes, dislikes, and other relevant information.")
    st.header("Data Management using MongoDB and SQL:")
    st.caption("The flexibility of MongoDB allows for the storage of unstructured or semi-structured data, accommodating the dynamic nature of YouTube data.")
    st.header("Data Processing and Transformation:")
    st.caption("Python scripts are implemented to clean, preprocess, and transform raw data before it is stored in MongoDB and SQL databases.")
    
channel_id=st.text_input("Enter the channel ID")\

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])
        
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exsits")
        
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
    
show_table=st.selectbox("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()
    
elif show_table=="PLAYLISTS":
    show_playlists_table()
    
elif show_table=="VIDEOS":
    show_videos_table()
    
elif show_table=="COMMENTS":
    show_comments_table()

#SQL_Connection

mydb=psycopg2.connect(host="localhost",
                     user="postgres",
                     password="Born@1998",
                     database="youtube_data",
                     port="5432")
cursor=mydb.cursor()

question=st.selectbox(
    "SELECT YOUR QUESTIONS",
    ("1. What are the names of all the videos and their corresponding channels?",
     "2. Which channels have the most number of videos, and how many videos do they have?",
     "3. What are the top 10 most viewed videos and their respective channels?",
     "4. How many comments were made on each video, and what are their corresponding video names?",
     "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
     "6. What is the total number of likes for each video, and what are their corresponding video names?",
     "7. What is the total number of views for each channel, and what are their corresponding channel names?",
     "8. What are the names of all the channels that have published videos in the year 2022?",
     "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
     "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_video from channels
                order by total_videos desc limit 1'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

    # Plotting using Matplotlib
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create a horizontal bar chart
    bars = ax.barh(df3['videotitle'], df3['views'], color='orange')

    # Invert the y-axis to have the highest views at the top
    ax.invert_yaxis()

    # Add labels and title
    ax.set_xlabel('Views')
    ax.set_ylabel('Video Title')
    ax.set_title('Top 10 Videos by Views')

    # Add values on top of the bars
    for bar in bars:
        plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, 
                f'{int(bar.get_width()):,}', 
                va='center', ha='left', color='black')

    # Show the plot
    st.pyplot(fig)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle, channel_name as channelname, likes as likecount
                from videos where likes is not null 
                order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video title","channel name","like count"])
    st.write(df5)

elif question=="6. What is the total number of likes for each video, and what are their corresponding video names?":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["like count","video title"])
    st.write(df6)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select views as totalview, channel_name as channelname from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["total views","channel name"])
    st.write(df7)

    # Plotting using Matplotlib
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create a bar chart
    ax.bar(df7['channel name'], df7['total views'], color='skyblue')

    # Add labels and title
    ax.set_xlabel('Channels')
    ax.set_ylabel('Total Views')
    ax.set_title('Total Views by Channel')

    # Rotate x-axis labels for better visibility
    plt.xticks(rotation=45, ha='right')

    # Show the plot
    st.pyplot(fig)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as videotitle,published_date as releasedate,channel_name as channelname from videos
                where extract(year from published_date)=2022 '''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","Publishd Date","channel name"])
    st.write(df8)

    # Plotting using Matplotlib
    fig, ax = plt.subplots(figsize=(8, 8))

    # Count the number of videos per channel
    channel_counts = df8['channel name'].value_counts()

    # Create a pie chart
    ax.pie(channel_counts, labels=channel_counts.index, autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)

    # Add title
    ax.set_title('Distribution of Videos Released in 2022 by Channel')

    # Show the plot
    st.pyplot(fig)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration 
        FROM videos GROUP BY channel_name'''

    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    # Convert duration to timestamp format
    df9['averageduration'] = df9['averageduration'].astype(str)

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration))

    df1 = pd.DataFrame(T9)
    st.write(df1)

    # Plotting using Matplotlib
    fig, ax = plt.subplots(figsize=(10, 6))

    # Convert average duration to seconds for plotting
    df9['averageduration'] = pd.to_timedelta(df9['averageduration']).dt.total_seconds()

    # Create a line graph
    ax.plot(df9['channelname'], df9['averageduration'], marker='o', linestyle='-', color='b')

    # Add labels and title
    ax.set_xlabel('Channel Name')
    ax.set_ylabel('Average Duration (seconds)')
    ax.set_title('Average Video Durations by Channel')

    # Rotate x-axis labels for better visibility
    plt.xticks(rotation=45, ha='right')

    # Show the plot
    st.pyplot(fig)

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as videotitle, channel_name as channelname, comments as comments
                from videos order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video titles","channel name","comment"])
    st.write(df10)