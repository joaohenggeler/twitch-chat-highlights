#!/usr/bin/env python3

"""
	@TODO
"""

import json
import sqlite3
import sys
from collections import Counter, namedtuple
from datetime import datetime, timedelta

import twitch

####################################################################################################

with open('config.json') as file:
	CONFIG = json.loads(file.read())

helix = twitch.Helix(CONFIG['client_id'], bearer_token=CONFIG['access_token'], use_cache=True)
channel_name = CONFIG['highlights']['channel'].lower()
user = helix.user(channel_name)

try:
	db = sqlite3.connect(CONFIG['database_filename'])
	db.isolation_level = None
	db.row_factory = sqlite3.Row
	print(f'Connected to the database: ' + CONFIG['database_filename'])
except sqlite3.Error as error:
	print(f'Failed to create or open the database with the error: {repr(error)}')
	sys.exit(1)

try:
	cursor = db.execute('SELECT Id FROM Channel WHERE Name = :name;', {'name': channel_name})
	row = cursor.fetchone()
	if row:
		channel_id = row['Id']
	else:
		print(f'Could not find the channel {channel_name} in the database.')
		sys.exit(1)
except sqlite3.Error as error:
	print(f'Failed to get the ID for the channel {channel_name} with the error: {repr(error)}')
	sys.exit(1)

####################################################################################################

begin_date = datetime.strptime(CONFIG['highlights']['begin_date'], '%Y-%m-%d')
end_date = begin_date + timedelta(days=CONFIG['highlights']['num_days'])

begin_date = begin_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

if end_date < begin_date:
	begin_date, end_date = end_date, begin_date

for i, video in enumerate(user.videos(type='archive')):

	if video.created_at < begin_date:
		break
	elif begin_date <= video.created_at <= end_date:
		
		duration = video.duration.replace('h', ':').replace('m', ':').replace('s', '')
		hours, minutes, seconds = duration.split(':')
		hours, minutes, seconds = int(hours), int(minutes), int(seconds)
		duration = f'{hours:02}:{minutes:02}:{seconds:02}'

		creation_time = video.created_at.replace('Z', '+00:00')
		creation_time = datetime.fromisoformat(creation_time)
		end_time = creation_time + timedelta(hours=hours, minutes=minutes, seconds=seconds)
		
		creation_time = creation_time.strftime('%Y-%m-%d %H:%M:%S.%f')
		end_time = end_time.strftime('%Y-%m-%d %H:%M:%S.%f')

		params = {'channel_id': channel_id, 'twitch_id': video.id, 'title': video.title, 'creation_time': creation_time, 'duration': duration}

		try:		
			cursor = db.execute('''
								INSERT OR IGNORE INTO Video (ChannelId, TwitchId, Title, CreationTime, Duration)
								VALUES (:channel_id, :twitch_id, :title, :creation_time, :duration);
								''', params)

			db.execute(	'''
						UPDATE Chat SET VideoId = (SELECT Id FROM Video WHERE TwitchId = :twitch_id)
						WHERE ChannelId = :channel_id AND Timestamp BETWEEN :begin_time AND :end_time;
						''', {'twitch_id': video.id, 'channel_id': channel_id, 'begin_time': creation_time, 'end_time': end_time})

		except sqlite3.Error as error:
			print(f'Failed to insert the video {video.id} ({video.title}) with the error: {repr(error)}')	

print(f'The remaining rate limit is {helix.api.rate_limit_remaining} of {helix.api.rate_limit_points} points.')