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
from varname import nameof
import urllib.request
from instagrapi.exceptions import LoginRequired

# useful info in media :
# pk code (real media ID)
# id (media ID + user ID)
# taken_at -> datetime.datetime
# comment_count
# like_count
# usertags (list de usertag)
# resources (list of resource)

@task (name='Loading ENV',
       description='loading data from env file')
def load_environment_variables ():
    load_dotenv()
    PATH_DIRECTORY_IMAGES = os.getenv ('PATH_DIRECTORY_IMAGES')
    PATH_DIRECTORY_CSV = os.getenv('PATH_DIRECTORY_CSV')
    USERNAMES = os.getenv ('USERNAMES')
    INSTAGRAM_USERNAME = os.getenv ('INSTAGRAM_USERNAME')
    INSTAGRAM_PASSWORD = os.getenv ('INSTAGRAM_PASSWORD')
    SESSION_FILE_PATH = os.getenv ('SESSION_FILE_PATH')

    return PATH_DIRECTORY_IMAGES, PATH_DIRECTORY_CSV, USERNAMES, INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH


@task (name='Get Data',
       description='Retrieve data from profiles',
       cache_key_fn=task_input_hash, 
       cache_expiration=datetime.timedelta(days=1))
def get_data (usernames: str, client: object):
    NUMBER_MEDIAS = 20
    usernames_list = usernames.split(',')
    medias_info = {
        'username': [],
        'like_count': [],
        'comment_count': [],
        'pk_resources': [],
        'taken_at': []
    }

    profiles_info = {
        'username': usernames_list,
        'follower_count': [],
        'media_count': []
    }

    images_info = {
        'pk_media': [],
        'pk_image': [],
        'url': []
    }

    for username in usernames_list:
        infos_dict = client.user_info_by_username(username).dict()
        profiles_info['follower_count'].append (infos_dict['follower_count'])
        profiles_info['media_count'].append (infos_dict['media_count'])

        user_id = client.user_id_from_username(username)
        medias = client.user_medias(user_id, NUMBER_MEDIAS)
        for media in medias:
            media_dict = media.dict()
            medias_info['username'].append (username)
            medias_info['comment_count'].append (media_dict['comment_count'])
            medias_info['like_count'].append (media_dict['like_count'])
            medias_info['pk_resources'].append (media_dict['pk'])
            medias_info['taken_at'].append (media_dict['taken_at'].date().isoformat())

            if media_dict['resources']:
                for media in media_dict['resources']:
                    images_info['pk_media'].append (media_dict['pk'])
                    images_info['pk_image'].append (media['pk'])
                    images_info['url'].append (media['thumbnail_url'])
            else :
                images_info['pk_media'].append (media_dict['pk'])
                images_info['pk_image'].append (media_dict['pk'])
                images_info['url'].append (media_dict['thumbnail_url'])

    return profiles_info, medias_info, images_info


@task (name='Create Dataframe',
       description='Create a Pandas dataframe.')
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
    
    try:
        client.account_info()
    except LoginRequired:
        client.relogin()

    client.dump_settings(session_file_path)
    return client


@task (name='Data to CSV')
def write_data_into_csv (data: pd.DataFrame, dict_name: str, path_to_directory: str):
    if dict_name == 'df_medias':
        if not Path(path_to_directory + '/medias.csv').is_file():
            data.to_csv(path_to_directory + '/medias.csv', encoding='utf-8', index=False)
        else:
            csv_file = pd.read_csv(path_to_directory + '/medias.csv')
            filter_column_like = csv_file['like_count'] != data['like_count']
            filter_column_comment = csv_file['comment_count'] != data['comment_count']

            csv_file.loc [filter_column_like, 'like_count'] = data['like_count']
            csv_file.loc [filter_column_comment, 'comment_count'] = data['comment_count']
            csv_file.to_csv(path_to_directory + '/medias.csv', encoding='utf-8', index=False)

    elif dict_name == 'df_profiles' : # profiles
        if not Path(path_to_directory + '/profiles.csv').is_file():
            data.to_csv(path_to_directory + '/profiles.csv', encoding='utf-8', index=False)
        else:
            csv_file = pd.read_csv(path_to_directory + '/profiles.csv')
            filter_column_follower = csv_file['follower_count'] != data['follower_count']
            filter_column_media = csv_file['media_count'] != data['media_count']

            csv_file.loc [filter_column_follower, 'follower_count'] = data['follower_count']
            csv_file.loc [filter_column_media, 'media_count'] = data['media_count']
            csv_file.to_csv(path_to_directory + '/profiles.csv', encoding='utf-8', index=False)
            
    else: 
        if not Path(path_to_directory + '/images.csv').is_file():
            data.to_csv(path_to_directory + '/images.csv', encoding='utf-8', index=False)
        else:
            csv_file = pd.read_csv (path_to_directory + '/images.csv')
            i = 0
            for _, row in data.iterrows():
                if not csv_file['pk_image'].eq(int(row['pk_image'])).any() :
                    csv_file = csv_file.append (row, ignore_index=True)
                    i += 1
            csv_file.to_csv(path_to_directory + '/images.csv', encoding='utf-8', index=False)
            print (f"add {i} rows in the file")


@task (name='Download images',
       description='Download images from profiles and store them in PATH_DIRECTORY_IMAGES')
def download_images (data: pd.DataFrame, local_destination: str) :
    for _, row in data.iterrows():
        pk_code = row['pk_image']
        if not Path(local_destination + f'/{pk_code}.jpg').is_file():
            link = row['url']
            resultFilePath, responseHeaders = urllib.request.urlretrieve(link, local_destination + f'/{pk_code}.jpg')


@flow (flow_run_name='{name}-on-{date}')
def my_flow (name: str, date: datetime.datetime):
    PATH_DIRECTORY_IMAGES, PATH_DIRECTORY_CSV, USERNAMES, INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH = load_environment_variables()
    client = instagram_connection (INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD, SESSION_FILE_PATH)

    profiles, medias, images = get_data (USERNAMES, client)
    df_profiles = create_dataframes (profiles)
    df_medias = create_dataframes (medias)
    df_images = create_dataframes (images)

    write_data_into_csv (df_profiles, nameof(df_profiles), PATH_DIRECTORY_CSV)
    write_data_into_csv (df_medias, nameof(df_medias), PATH_DIRECTORY_CSV)
    write_data_into_csv (df_images, nameof(df_images), PATH_DIRECTORY_CSV)
    download_images (df_images, PATH_DIRECTORY_IMAGES)
    send_logs_to_discord()

if __name__ == '__main__':
    my_flow(name="Name", date=datetime.datetime.utcnow())
