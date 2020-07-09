import json
import os
from celery import Celery
import shutil
from youtubeclient import get_authenticated_service, upload_vibe

app = Celery('upload_youtube_task', broker='amqp://guest@localhost//')


def create_snippet_from_vibe_description_file(title, description, video_file_path, keywords, category_id, status):
    snippet_data = dict(snippet=dict(title=title,
                                     description=description,
                                     tags=keywords,
                                     categoryId=category_id
                                     ),
                        status=dict(privacyStatus=status),
                        vibeFilePath=video_file_path
                        )

    snippet_file_path = '/tmp/' + title + '_snippet.json'

    with open(snippet_file_path, 'w') as json_file:
        json.dump(snippet_data, json_file, indent=2, sort_keys=True)

    return snippet_file_path

def insert_video_playlist (playlist_id,video_id):
    youtube = get_authenticated_service()
    request = youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "position": 0,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }
    )
    response = request.execute()
    return response

#Upload worker task
@app.task
def upload_to_youtube(title, description, video_file_path, keywords='', category='22', status='private',playlist_id=None):

    youtube = get_authenticated_service()

    snippet_file_path = create_snippet_from_vibe_description_file(title, description, video_file_path, keywords,
                                                                  category, status)

    if os.path.exists(snippet_file_path):
        response = upload_vibe(youtube, snippet_file_path)
        if playlist_id !=None:
            # Add to playlist
            insert_video_playlist(playlist_id, response['id'])
        return response

    else:
        print('Impossible to upload file to youtube snippet missing')
        return False


