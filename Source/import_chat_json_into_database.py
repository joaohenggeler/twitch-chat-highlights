#!/usr/bin/env python3

"""
	Imports one or more JSON files with a Twitch VOD's chat log into the database. Note that there's no protection for inserting the same data twice.
"""

import json
import sqlite3
import sys
from argparse import ArgumentParser
from datetime import datetime
from glob import glob

from twitch import Helix # type: ignore

from common import CommonConfig, split_twitch_duration, convert_twitch_timestamp_to_datetime

####################################################################################################

parser = ArgumentParser(description='Imports one or more JSON files with a Twitch VOD\'s chat log into the database. Note that there\'s no protection for inserting the same data twice.')
parser.add_argument('json_search_path', help=r'Where to search for the JSON files. May include wildcards to import multiple files. E.g. "C:\Path\chat.json" or "C:\Path\*.json".')
parser.add_argument('json_encoding', nargs='?', default='utf-8', help='The character encoding used by the JSON files. Defaults to "%(default)s". See a list of possible encodings here: https://docs.python.org/3/library/codecs.html#standard-encodings')
args = parser.parse_args()

config = CommonConfig()
helix = Helix(config.client_id, bearer_token=config.access_token, use_cache=True)

try:
	db = config.connect_to_database()
	print(f'Connected to the database: {config.database_filename}')
except sqlite3.Error as error:
	print(f'Failed to connect to the database with the error: {repr(error)}')
	sys.exit(1)

print()

file_path_list = glob(args.json_search_path)
for i, file_path in enumerate(file_path_list):

	with open(file_path, encoding=args.json_encoding) as file:
		chat_list = json.load(file)

	if not chat_list:
		print(f'Skipped the empty chat log from "{file_path}".')
		continue

	content_type = chat_list[0]['content_type']
	if content_type != 'video':
		print(f'Skipped the chat log from "{file_path}" with the unhandled content type "{content_type}".')
		continue

	video_twitch_id = chat_list[0]['content_id']

	try:
		video = helix.video(video_twitch_id)
	except Exception as error:
		print(f'Could not retrieve the video {video_twitch_id} for the chat log from "{file_path}" with the error: {repr(error)}')
		continue

	channel_name = video.user_name.lower()

	creation_time = video.created_at.replace('Z', '+00:00')
	creation_time = datetime.fromisoformat(creation_time).strftime('%Y-%m-%d %H:%M:%S.%f')

	hours, minutes, seconds, _ = split_twitch_duration(video.duration)
	duration = f'{hours:02}:{minutes:02}:{seconds:02}'

	print(f'Importing the chat log {i+1} of {len(file_path_list)} for the user "{channel_name}" ({video_twitch_id} at {creation_time}) with {len(chat_list)} messages from "{file_path}"...')

	try:
		db.execute('INSERT OR IGNORE INTO Channel (Name) VALUES (:name);', {'name': channel_name})
	except sqlite3.Error as error:
		print(f'- Failed to insert the channel "{channel_name}" with the error: {repr(error)}')
		continue

	try:		
		db.execute(	'''
					INSERT OR IGNORE INTO Video (ChannelId, TwitchId, Title, CreationTime, Duration)
					VALUES ((SELECT CL.Id FROM Channel CL WHERE CL.Name = :channel_name), :twitch_id, :title, :creation_time, :duration);
					''',
					{'channel_name': channel_name, 'twitch_id': video_twitch_id, 'title': video.title, 'creation_time': creation_time, 'duration': duration})

	except sqlite3.Error as error:
		print(f'- Failed to insert the video {video_twitch_id} ({video.title}) with the error: {repr(error)}')
		continue

	for j, chat in enumerate(chat_list):		

		creation_datetime = convert_twitch_timestamp_to_datetime(chat['created_at'])
		timestamp = creation_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
		message = chat['message']['body']

		try:
			db.execute(	'''
						INSERT INTO Chat (ChannelId, Timestamp, Message)
						VALUES ((SELECT CL.Id FROM Channel CL WHERE CL.Name = :channel_name), :timestamp, :message);
						''', {'channel_name': channel_name, 'timestamp': timestamp, 'message': message})
		except sqlite3.Error as error:
			print(f'- Failed to insert message {j+1} of {len(chat_list)} ({timestamp}, "{message}")')
	
if not file_path_list:
	print(f'Could not find any chat logs in "{args.json_search_path}".')

print()
print(f'The remaining API rate limit is {helix.api.rate_limit_remaining} of {helix.api.rate_limit_points} points.')
print('Finished running.')