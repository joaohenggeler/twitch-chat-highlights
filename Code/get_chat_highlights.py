#!/usr/bin/env python3

"""
	@TODO
"""

import json
import sqlite3
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from math import ceil, floor

import matplotlib.pyplot as plt
import twitch
from matplotlib.dates import DateFormatter

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
		print(f'Could not find the channel "{channel_name}" in the database.')
		sys.exit(1)
except sqlite3.Error as error:
	print(f'Failed to get the ID for the channel "{channel_name}" with the error: {repr(error)}')
	sys.exit(1)

####################################################################################################

begin_datetime = datetime.strptime(CONFIG['highlights']['begin_date'], '%Y-%m-%d')
end_datetime = begin_datetime + timedelta(days=CONFIG['highlights']['num_days'])

begin_date = begin_datetime.strftime('%Y-%m-%d')
end_date = end_datetime.strftime('%Y-%m-%d')

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

		try:		
			db.execute(	'''
						INSERT OR IGNORE INTO Video (ChannelId, TwitchId, Title, CreationTime, Duration)
						VALUES (:channel_id, :twitch_id, :title, :creation_time, :duration);
						''',
						{'channel_id': channel_id, 'twitch_id': video.id, 'title': video.title, 'creation_time': creation_time, 'duration': duration})

		except sqlite3.Error as error:
			print(f'Could not insert the video {video.id} ({video.title}) with the error: {repr(error)}')

		try:
			db.execute(	'''
						UPDATE Chat SET VideoId = (SELECT Id FROM Video WHERE TwitchId = :twitch_id)
						WHERE VideoId IS NULL AND ChannelId = :channel_id AND Timestamp BETWEEN :begin_time AND :end_time;
						''',
						{'twitch_id': video.id, 'channel_id': channel_id, 'begin_time': creation_time, 'end_time': end_time})

		except sqlite3.Error as error:
			print(f'Could not update the chat for the video {video.id} ({video.title}) with the error: {repr(error)}')

####################################################################################################

try:
	cursor = db.execute('SELECT * FROM Video WHERE ChannelId = :channel_id AND CreationTime BETWEEN :begin_date AND :end_date;',
						{'channel_id': channel_id, 'begin_date': begin_date, 'end_date': end_date})
	
	video_list = [dict(row) for row in cursor]
except sqlite3.Error as error:
	print(f'Failed to retrieve the videos between {begin_date} and {end_date} with the error: {repr(error)}')
	sys.exit(1)

print(f'Found {len(video_list)} videos in the "{channel_name}" channel between {begin_date} and {end_date}.')
print()

bucket_length = CONFIG['highlights']['bucket_length']
message_threshold = CONFIG['highlights']['message_threshold']
highlight_types = CONFIG['highlights']['types']

for highlight in highlight_types:
	lower_word_list = [word.lower() for word in highlight['words']]
	highlight.update({'words': lower_word_list})

for i, video_row in enumerate(video_list):

	twitch_id = video_row['TwitchId']
	print(f'- Processing video {i+1} of {len(video_list)} ({twitch_id})...')

	hours, minutes, seconds = video_row['Duration'].split(':')
	hours, minutes, seconds = int(hours), int(minutes), int(seconds)
	duration_in_seconds = hours * 3600 + minutes * 60 + seconds
	duration = f'{hours}h{minutes:02}m{seconds:02}s'

	num_buckets = ceil(duration_in_seconds / bucket_length)
	
	video_row['frequency'] = {}
	for highlight in highlight_types:
		highlight_title = highlight['title']
		video_row['frequency'][highlight_title] = [0] * num_buckets

	try:
		cursor = db.execute('''
							SELECT
								CT.Message,
								CT.Timestamp,
								CAST((JulianDay(CT.Timestamp) - JulianDay(V.CreationTime)) * 24 * 60 * 60 AS INTEGER) AS Offset
							FROM Chat CT
							INNER JOIN Video V ON CT.VideoId = V.Id
							INNER JOIN Channel CL ON V.ChannelId = CL.Id
							WHERE V.Id = :video_id
							ORDER BY CT.Timestamp;
							''', {'video_id': video_row['Id']})

	except sqlite3.Error as error:
		print(f'Could not retrieve the chat with the error: {repr(error)}')
		continue

	for chat_row in cursor:

		assert 	begin_date <= chat_row['Timestamp'] <= end_date, 'The chat message was not sent during the live stream.'

		word_list = chat_row['Message'].lower().split()
		bucket = floor(chat_row['Offset'] / bucket_length)

		for highlight in highlight_types:
			highlight_title = highlight['title']
			for word in word_list:
				if word in highlight['words']:
					video_row['frequency'][highlight_title][bucket] += 1
					break

	title = video_row['Title']
	creation_time = video_row['CreationTime']
	
	creation_datetime = datetime.fromisoformat(creation_time)
	end_datetime = creation_datetime + timedelta(seconds=duration_in_seconds)

	figure, axis = plt.subplots(figsize=(12, 6))

	for highlight in highlight_types:
		highlight_title = highlight['title']
		y_data = video_row['frequency'][highlight_title]
		x_data = [creation_datetime + timedelta(seconds=i*bucket_length) for i in range(len(y_data))]
		axis.plot(x_data, y_data, label=highlight_title, color=highlight['color'], linewidth=0.7)

	axis.axhline(y=message_threshold, linestyle='dashed', label=f'Threshold ({message_threshold})', color='k')
	axis.set_xlim(creation_datetime, end_datetime)

	creation_time = creation_datetime.strftime('%Y-%m-%d %H:%M:%S')
	video_url = f'https://www.twitch.tv/videos/{twitch_id}'
	axis.set(xlabel=f'Time in Buckets of {bucket_length} Seconds (UTC)', ylabel='Number of Messages', title=f'"{title}" ({creation_time}, {duration})\n{video_url}')
	axis.legend()
	
	datetime_formatter = DateFormatter("%Hh%M")
	axis.xaxis.set_major_formatter(datetime_formatter)

	figure.tight_layout()

	plot_filename = f'{channel_name}_{begin_date}_to_{end_date}_{twitch_id}.png'
	figure.savefig(plot_filename, dpi=200)
	print(f'- Saved the plot to "{plot_filename}".')

####################################################################################################
print(f'Summarizing the top highlights.')
print()

summary_text = f'Twitch Chat Highlights ({begin_date} to {end_date}):\n\n'
Candidate = namedtuple('Candidate', ['TwitchId', 'TimeDelta', 'Count'])

for highlight in highlight_types:
	highlight_title = highlight['title']
	highlight_top = highlight['top']

	highlight_candidates = []
	for video_row in video_list:	
		frequency = video_row['frequency'][highlight_title]
		candidates = [Candidate(video_row['TwitchId'], timedelta(seconds=i*bucket_length), count) for i, count in enumerate(frequency) if count > message_threshold]
		highlight_candidates.extend(candidates)

	highlight_candidates = sorted(highlight_candidates, key=lambda x: x.Count, reverse=True)
	highlight_candidates[:highlight_top]

	summary_text += f'{highlight_title} (most ' + ', '.join(highlight['words']) + '):\n'

	if highlight_candidates:

		for i, candidate in enumerate(highlight_candidates):
			timestamp = candidate.TimeDelta - timedelta(seconds=CONFIG['highlights']['top_url_delay'])
			timestamp = str(timestamp).replace(':', 'h', 1).replace(':', 'm', 1) + 's'
			highlight_url = f'https://www.twitch.tv/videos/{candidate.TwitchId}?t={timestamp}'
			summary_text += f'{i+1}. {highlight_url} ({candidate.Count})\n'

	else:
		summary_text += 'No highlights found.\n'

	summary_text += '\n'

	print(f'- Found {len(highlight_candidates)} "{highlight_title}" highlights.')

print()

summary_filename = f'{channel_name}_{begin_date}_to_{end_date}.txt'
with open(summary_filename, 'w', encoding='utf-8') as file:
	file.write(summary_text)

print(f'Saved the summary to "{summary_filename}".')

####################################################################################################

print()
print(f'The remaining API rate limit is {helix.api.rate_limit_remaining} of {helix.api.rate_limit_points} points.')
print('Finished running.')