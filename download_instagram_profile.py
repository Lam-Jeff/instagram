import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from dotenv import load_dotenv
import datetime
from prefect import flow
from prefect_discord import DiscordWebhook
import instagrapi
import pandas as pd
from pathlib import Path

# useful info in media :
# pk code (real media ID)
# id (media ID + user ID)
# taken_at -> datetime.datetime
# comment_count
# like_count
# usertags (list de usertag)
# resources (list of resource)

# 1 table for all profiles, general infos
# 1 table for each profile

#max is 95 - to do only get everything once

@task (name='Loading ENV',
       description='loading data from env file')
def load_environment_variables ():
    load_dotenv()
    PATH = os.getenv ('PATH')
    USERNAMES = os.getenv ('USERNAMES')
    INSTAGRAM_USERNAME = os.getenv ('INSTAGRAM_USERNAME')
    INSTAGRAM_PASSWORD = os.getenv ('INSTAGRAM_PASSWORD')
    SESSION_FILE_PATH = os.getenv ('SESSION_FILE_PATH')

    return PATH, USERNAMES, INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH


@task (name='Get Data',
       description='Retrieve data from profiles',
       cache_key_fn=task_input_hash, 
       cache_expiration=datetime.timedelta(days=1))
def get_data (usernames: str, client: object):
    NUMBER_MEDIAS = 20
    usernames_list = usernames.split(',')
    medias_info = {
        'username': [],
        'like_counts': [],
        'comment_counts': [],
        'resources': [],
        'taken_at': []
    }

    profiles_info = {
        'username': usernames_list,
        'follower_counts': [],
        'media_counts': []
    }

    for username in usernames_list:
        infos_dict = client.user_info_by_username(username).dict()
        profiles_info['follower_counts'].append (infos_dict['follower_count'])
        profiles_info['media_counts'].append (infos_dict['media_count'])

        user_id = client.user_id_from_username(username)
        medias = client.user_medias(user_id, NUMBER_MEDIAS)
        for media in medias:
            media_dict = media.dict()
            medias_info['username'].append (username)
            medias_info['comment_counts'].append (media_dict['comment_count'])
            medias_info['like_counts'].append (media_dict['like_count'])
            medias_info['resources'].append (media_dict['resources'] or media_dict['thumbnail_url'])
            medias_info['taken_at'].append (media_dict['taken_at'].date().isoformat())

    return profiles_info, medias_info


@task
def create_dataframes (data: dict):
    df = pd.DataFrame(data)
    print (df.head(10))
    return df


@task (name='Message Discord',
       description='send a message to personal Discord after flow execution')
def send_logs_to_discord ():
    discord_webhook_block = DiscordWebhook.load("personal-discord")
    discord_webhook_block.notify(f"Flow executed ! date: {datetime.datetime.utcnow()}")


@task (name='Instagram connection',
       description='Connection to an instagram account')
def instagram_connection (username: str, password: str, session_file_path: str):
    client = instagrapi.Client()

    if Path(session_file_path).is_file():
        client.load_settings(session_file_path)
        client.login (username, password)
    else:    
        client.login (username, password)
        client.dump_settings(session_file_path)
    return client

@flow (name='Save data')
def save_data_subflow ():
    pass

@flow (flow_run_name='{name}-on-{date}')
def my_flow (name: str, date: datetime.datetime):
    PATH, USERNAMES, INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH = load_environment_variables()
    client = instagram_connection (INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH)

    profiles, medias = get_data (USERNAMES, client)
    df_profiles = create_dataframes (profiles)
    df_medias = create_dataframes (medias)
    df_profiles.to_csv('profiles.csv', encoding='utf-8', index=False)
    df_medias.to_csv('medias.csv', encoding='utf-8', index=False)
    send_logs_to_discord()

if __name__ == '__main__':
    my_flow(name="Name", date=datetime.datetime.utcnow())

