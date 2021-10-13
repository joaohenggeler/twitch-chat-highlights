#!/usr/bin/env python3

"""
	@TODO
"""

import json
import re
import sqlite3
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from math import ceil, floor

import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator, MultipleLocator
from twitch import Helix

####################################################################################################

# Read the configurations file, connect to the database, and setup the Twitch API for a given channel.

with open('config.json') as file:
	CONFIG = json.load(file)

helix = Helix(CONFIG['client_id'], bearer_token=CONFIG['access_token'], use_cache=True)
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

# Find any VODs in a given time period and assign the correct video IDs to the previously collected
# chat messages.

begin_datetime = datetime.strptime(CONFIG['highlights']['begin_date'], '%Y-%m-%d')
end_datetime = begin_datetime + timedelta(days=CONFIG['highlights']['num_days'])

begin_date = begin_datetime.strftime('%Y-%m-%d')
end_date = end_datetime.strftime('%Y-%m-%d')

# For a negative number of days.
if end_date < begin_date:
	begin_date, end_date = end_date, begin_date

# Search in the Past Broadcasts section.
for i, video in enumerate(user.videos(type='archive')):

	if video.created_at < begin_date:
		break
	elif begin_date <= video.created_at <= end_date:
		
		# Duration format: 00h00m00s
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

# Iterate over each VOD and its chat for the requested time period. For each VOD, we'll count the
# number of times a specific word/emote was sent in the chat, and also generate a plot with each
# highlight category's word frequency.

try:
	cursor = db.execute('SELECT * FROM Video WHERE ChannelId = :channel_id AND CreationTime BETWEEN :begin_date AND :end_date;',
						{'channel_id': channel_id, 'begin_date': begin_date, 'end_date': end_date})
	
	video_list = [dict(row) for row in cursor]
except sqlite3.Error as error:
	print(f'Failed to retrieve the videos between {begin_date} and {end_date} with the error: {repr(error)}')
	sys.exit(1)

if not video_list:
	print(f'Could not find any videos in the "{channel_name}" channel between {begin_date} and {end_date}.')
	sys.exit(1)

print(f'Found {len(video_list)} videos in the "{channel_name}" channel between {begin_date} and {end_date}.')
print()

bucket_length = CONFIG['highlights']['bucket_length']
message_threshold = CONFIG['highlights']['message_threshold']
highlight_types = CONFIG['highlights']['types']

for highlight in highlight_types:
	
	search_words = []
	for word in highlight['words']:
		
		if word.startswith('regex:'):
			_, word = word.split('regex:', 1)
			word = re.compile(word, re.IGNORECASE)
		else:
			word = word.lower()
		
		search_words.append(word)

	highlight['search_words'] = search_words

for i, video in enumerate(video_list):

	twitch_id = video['TwitchId']
	print(f'- Processing video {i+1} of {len(video_list)} ({twitch_id})...')

	video['CreationDateTime'] = datetime.fromisoformat(video['CreationTime'])

	hours, minutes, seconds = video['Duration'].split(':')
	hours, minutes, seconds = int(hours), int(minutes), int(seconds)
	duration_in_seconds = hours * 3600 + minutes * 60 + seconds
	num_buckets = ceil(duration_in_seconds / bucket_length)
	
	video['frequency'] = {}
	for highlight in highlight_types:
		name = highlight['name']
		video['frequency'][name] = [0] * num_buckets

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
							''', {'video_id': video['Id']})

	except sqlite3.Error as error:
		print(f'Could not retrieve the chat with the error: {repr(error)}')
		continue

	for chat in cursor:

		assert begin_date <= chat['Timestamp'] <= end_date, 'The chat message was not sent during the live stream.'

		word_list = chat['Message'].lower().split()
		bucket = floor(chat['Offset'] / bucket_length)

		for highlight in highlight_types:
			
			name = highlight['name']
			skip_to_next_highlight = False

			for word in word_list:
				
				for search_word in highlight['search_words']:

					match = False
					if isinstance(search_word, str):
						match = (word == search_word)
					else:
						match = search_word.match(word)

					if match:
						video['frequency'][name][bucket] += 1
						skip_to_next_highlight = True
						break					

				if skip_to_next_highlight:
					break

	# Plot the word frequency.

	figure, axis = plt.subplots(figsize=(12, 6))

	for highlight in highlight_types:
		name = highlight['name']
		y_data = video['frequency'][name]
		x_data = [i*bucket_length for i in range(len(y_data))]
		axis.plot(x_data, y_data, label=name, color=highlight['color'], linewidth=0.7)

	axis.axhline(y=message_threshold, linestyle='dashed', label=f'Threshold ({message_threshold})', color='k')

	title = video['Title']
	creation_time = video['CreationDateTime'].strftime('%Y-%m-%d %H:%M:%S')
	duration = f'{hours}h{minutes:02}m{seconds:02}s'
	video_url = f'https://www.twitch.tv/videos/{twitch_id}'

	axis.set(xlabel=f'Time in Buckets of {bucket_length} Seconds', ylabel='Number of Messages', title=f'"{title}" ({creation_time}, {duration})\n{video_url}')
	axis.legend()
	
	# Format the number of seconds as 00h00.
	def seconds_formatter(num_seconds, position):
		label, _ = str(timedelta(seconds=num_seconds)).rsplit(':', 1)
		return label.replace(':', 'h', 1)

	axis.xaxis.set_major_formatter(seconds_formatter)
	axis.xaxis.set_major_locator(MultipleLocator(30*60))
	axis.xaxis.set_minor_locator(AutoMinorLocator(4))	
	axis.yaxis.set_major_locator(MultipleLocator(5))

	axis.tick_params(axis='x', which='major', length=7)
	axis.tick_params(axis='x', which='minor', length=4)

	axis.set_ylim(-1)

	figure.tight_layout()

	creation_date = video['CreationDateTime'].strftime('%Y-%m-%d')
	plot_filename = f'{channel_name}_{creation_date}_{twitch_id}.png'
	figure.savefig(plot_filename, dpi=200)
	print(f'- Saved the plot to "{plot_filename}".')

print()

####################################################################################################

# Compare some of the previous categories against each other and compute the final balance. E.g. the number of +2 vs -2.

compare_types = CONFIG['highlights']['compare']
for compare in compare_types:
	compare['positive_highlight'] = next(highlight for highlight in highlight_types if highlight['name'] == compare['positive'])
	compare['negative_highlight'] = next(highlight for highlight in highlight_types if highlight['name'] == compare['negative'])

for video in video_list:
	for compare in compare_types:

		positive_highlight_name = compare['positive_highlight']['name']
		negative_highlight_name = compare['negative_highlight']['name']

		positive_frequency = video['frequency'][positive_highlight_name]
		negative_frequency = video['frequency'][negative_highlight_name]

		positive_balance_name  = compare['positive_name']
		negative_balance_name  = compare['negative_name']
		controversial_balance_name  = compare['controversial_name']

		def measure_controversy_1(positive_count: int, negative_count: int) -> float:
			return (positive_count + negative_count) / (abs(positive_count - negative_count) + 1)

		def measure_controversy_2(positive_count: int, negative_count: int) -> float:
			if positive_count == 0 or negative_count == 0:
				return 0
			else:
				return (positive_count + negative_count) ** (min(positive_count, negative_count) / max(positive_count, negative_count))

		video['frequency'][positive_balance_name] = [positive_count - negative_count for positive_count, negative_count in zip(positive_frequency, negative_frequency)]
		video['frequency'][negative_balance_name] = video['frequency'][positive_balance_name].copy()
		video['frequency'][controversial_balance_name] = [measure_controversy_2(positive_count, negative_count) for positive_count, negative_count in zip(positive_frequency, negative_frequency)]

for compare in compare_types:
	for kind in ['positive', 'negative', 'controversial']:

		balance_highlight = {}

		balance_highlight['name'] = compare[kind + '_name']
		balance_highlight['top'] = compare[kind + '_top']
		balance_highlight['positive_words'] = compare['positive_highlight']['words']
		balance_highlight['negative_words'] = compare['negative_highlight']['words']
		balance_highlight['compare_kind'] = kind

		highlight_types.append(balance_highlight)

####################################################################################################

# Create a text file linking to the top highlights for each word/emote category.

print(f'Summarizing the top highlights.')
print()

top_url_delay = CONFIG['highlights']['top_url_delay']
top_bucket_distance_threshold = CONFIG['highlights']['top_bucket_distance_threshold']

summary_text = f'**Twitch Chat Highlights ({begin_date} to {end_date}):**\n\n&nbsp;\n\n'
Candidate = namedtuple('Candidate', ['Video', 'Bucket', 'Count'])

for highlight in highlight_types:

	if highlight.get('skip_summary'):
		continue

	name = highlight['name']
	top = highlight['top']
	compare_kind = highlight.get('compare_kind', '')

	highlight_candidates = []
	for video in video_list:	
		
		frequency = video['frequency'][name]
		for i, count in enumerate(frequency):
			
			if count > message_threshold or compare_kind:
				
				candidate = Candidate(video, i, count)
				highlight_candidates.append(candidate)
		
	reverse_candidates = (compare_kind != 'negative')
	highlight_candidates = sorted(highlight_candidates, key=lambda x: x.Count, reverse=reverse_candidates)
	
	# Remove any candidates that occurred too close to each other, starting with the worst ones.
	# We don't have to do this step if we only want the best candidate, since that one is never
	# removed from the list. 
	if top_bucket_distance_threshold is not None and top > 1:
		
		num_removed = 0
		for worse_idx, worse_candidate in reversed(list(enumerate(highlight_candidates))):
			for better_candidate in highlight_candidates:
				
				# Skip the same candidate (since they're the same) and any other future candidates (since
				# we already compared them in previous iterations of the outer loop).
				if worse_candidate is better_candidate:
					break

				if abs(worse_candidate.Bucket - better_candidate.Bucket) < top_bucket_distance_threshold:
					# Remember that, since we're iterating backwards in the outer loop, we're removing
					# this element from the end of the list.
					del highlight_candidates[worse_idx]
					num_removed += 1
					break

		print(f'- Removed {num_removed} highlights that were at most {(top_bucket_distance_threshold - 1) * bucket_length} seconds apart.')

	highlight_candidates = highlight_candidates[:top]

	if compare_kind:
		summary_text += f'**{name}** ({compare_kind.title()} Balance: ' + ', '.join(highlight['positive_words']) + ' vs ' + ', '.join(highlight['negative_words']) + '):\n\n'
	else:
		summary_text += f'**{name}** (' + ', '.join(highlight['words']) + '):\n\n'

	if highlight_candidates:

		for i, candidate in enumerate(highlight_candidates):
			
			count = candidate.Count if isinstance(candidate.Count, int) else ('%.1f' % candidate.Count)
			weekday = candidate.Video['CreationDateTime'].strftime('%a (%d/%m)')

			# VOD timestamp format: 00h00m00s
			twitch_id = candidate.Video['TwitchId']
			timestamp = timedelta(seconds=candidate.Bucket * bucket_length) - timedelta(seconds=top_url_delay)
			timestamp = str(timestamp).replace(':', 'h', 1).replace(':', 'm', 1) + 's'

			highlight_url = f'https://www.twitch.tv/videos/{twitch_id}?t={timestamp}'

			summary_text += f'{i+1}. [{count}] {weekday}: [REPLACEME]({highlight_url})\n\n'

	else:
		summary_text += 'No highlights found.\n'

	summary_text += '\n'

	print(f'- Found {len(highlight_candidates)} "{name}" highlights.')

print()

summary_text = summary_text.rstrip()

summary_filename = f'{channel_name}_{begin_date}_to_{end_date}.txt'
with open(summary_filename, 'w', encoding='utf-8') as file:
	file.write(summary_text)

print(f'Saved the summary to "{summary_filename}".')

####################################################################################################

print()
print(f'The remaining API rate limit is {helix.api.rate_limit_remaining} of {helix.api.rate_limit_points} points.')
print('Finished running.')