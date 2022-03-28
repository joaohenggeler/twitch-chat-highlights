#!/usr/bin/env python3

"""
	Processes any saved chat messages in the database between two dates, generates a summary text file with the top highlights in
	different categories, and optionally creates images that plot chat's reactions during each live stream.

	Use the JSON configuration file next to this script to choose the Twitch channel, dates, categories, and other options.

	The previously mentioned chat messages are gathered by either running the bot in "chat_transcript_bot.py" during the
	stream, or by retriving the VOD's JSON chat logs from the Twitch API and importing them with "import_chat_json_into_database.py".
"""

import re
import sqlite3
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from math import ceil, floor
from typing import cast, List, Pattern, Union

import matplotlib.pyplot as plt # type: ignore
from matplotlib.ticker import AutoMinorLocator, MultipleLocator # type: ignore
from twitch import Helix # type: ignore

from common import CommonConfig, split_twitch_duration, convert_twitch_timestamp_to_datetime

####################################################################################################

class Category():

	# From the config file.
	name: str
	words: List[str]
	top: int
	color: str
	skip_summary: bool

	# Determined at runtime.
	search_words: List[Union[str, Pattern]]

	# For the CategoryBalance subclass.
	positive_words: List[str]
	negative_words: List[str]
	comparison_name: str
	comparison_kind: str

	def __init__(self, **kwargs):
		self.words = []
		self.skip_summary = False
		self.__dict__.update(kwargs)
	
		self.search_words = []
		for word in self.words:
			
			if word.startswith('regex:'):
				_, word = word.split('regex:', 1)
				word = re.compile(word, re.IGNORECASE)
			else:
				word = word.lower()
			
			self.search_words.append(word)

class CategoryBalance(Category):

	def __init__(self, **kwargs):
		super().__init__(**kwargs)

class CategoryComparison():

	# From the config file.
	name: str
	positive_category: 'Category'
	negative_category: 'Category'
	
	positive_name: str
	negative_name: str
	controversial_name: str
	
	positive_top: int
	negative_top: int
	controversial_top: int

	skip_summary: bool

	def __init__(self, **kwargs):
		self.skip_summary = False
		self.__dict__.update(kwargs)

class HighlightsConfig(CommonConfig):

	# From the config file.
	channel_name: str
	
	vod_criteria: str
	begin_date: str
	num_days: int
	notes: str

	get_vods_from_api: bool
	vod_type: str
	use_youtube_urls: bool

	bucket_length: int
	message_threshold: int
	top_bucket_distance_threshold: int
	top_url_delay: int
	
	plot_categories: Union[bool, list]
	plot_threshold: bool
	show_word_list: bool

	categories: List['Category']
	comparisons: List['CategoryComparison']

	# Determined at runtime.
	channel_database_id: int

	vods_begin_datetime: datetime
	vods_end_datetime: datetime
	vods_begin_date: str
	vods_end_date: str
	vods_begin_time: str
	vods_end_time: str
	
	vods_criteria_text: str
	vods_criteria_summary_title: str
	vods_criteria_filename_suffix: str

	def __init__(self):
		super().__init__()

		self.categories = []
		self.comparisons = []

		for key, value in self.json_config['highlights'].items():

			if key == 'categories':
				for category_params in value:
					category = Category(**category_params)
					self.categories.append(category)
			elif key == 'comparisons':
				for comparison_params in value:
					comparison = CategoryComparison(**comparison_params)
					self.comparisons.append(comparison)
			else:
				setattr(self, key, value)

		self.channel_name = self.channel_name.lower()

		# Exclude the last day from the date range since it includes midnight.
		self.vods_begin_datetime = datetime.strptime(self.begin_date, '%Y-%m-%d')
		self.vods_end_datetime = self.vods_begin_datetime + timedelta(days=self.num_days) - timedelta(seconds=1)

		# For a negative number of days.
		if self.vods_end_datetime < self.vods_begin_datetime:
			self.vods_begin_datetime, self.vods_end_datetime = self.vods_end_datetime, self.vods_begin_datetime

		self.vods_begin_date = self.vods_begin_datetime.strftime('%Y-%m-%d')
		self.vods_end_date = self.vods_end_datetime.strftime('%Y-%m-%d')
		self.vods_begin_time = self.vods_begin_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
		self.vods_end_time = self.vods_end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

		if self.vod_criteria == 'date':
			self.vods_criteria_text = f'between {self.vods_begin_date} and {self.vods_end_date}'
			self.vods_criteria_summary_title = f'{self.vods_begin_date} to {self.vods_end_date}'
			self.vods_criteria_filename_suffix = f'{self.vods_begin_date}_to_{self.vods_end_date}'

		elif self.vod_criteria == 'notes':
			self.vods_criteria_text = f'matching the notes "{self.notes}"'
			self.vods_criteria_summary_title = f'Matching "{self.notes}"'
			self.vods_criteria_filename_suffix = self.notes.replace(' ', '_')
		
		else:
			assert False, f'Unhandled VOD criteria "{self.vod_criteria}". Only "date" and "notes" are allowed.'

		for comparison in self.comparisons:
			# Turn the category name in the config file into the category object itself.
			comparison.positive_category = next(category for category in self.categories if category.name == comparison.positive_category)
			comparison.negative_category = next(category for category in self.categories if category.name == comparison.negative_category)

####################################################################################################

# Read the configurations file, connect to the database, and setup the Twitch API for a given channel.

config = HighlightsConfig()

try:
	db = config.connect_to_database()
	print(f'Connected to the database: {config.database_filename}')
except sqlite3.Error as error:
	print(f'Failed to connect to the database with the error: {repr(error)}')
	sys.exit(1)

try:
	cursor = db.execute('SELECT Id FROM Channel WHERE Name = :name;', {'name': config.channel_name})
	row = cursor.fetchone()
	if row:
		config.channel_database_id = row['Id']
	else:
		print(f'Could not find the channel "{config.channel_name}" in the database.')
		sys.exit(1)
except sqlite3.Error as error:
	print(f'Failed to get the ID for the channel "{config.channel_name}" with the error: {repr(error)}')
	sys.exit(1)

print()

####################################################################################################

# Find any VODs in a given time period and assign the correct video IDs to the previously collected
# chat messages.

if config.get_vods_from_api:

	if config.vod_criteria != 'date':
		print(f'Cannot find new VODs via the Twitch API while using the "{config.vod_criteria}" criteria. Only "date" is allowed.')
		sys.exit(1)

	helix = Helix(config.client_id, bearer_token=config.access_token, use_cache=True)
	helix_user = helix.user(config.channel_name)
	helix_video_list = []

	# Search the videos section (past broadcasts, highlights, uploads, or all).
	for video in helix_user.videos(type=config.vod_type):

		# Creation date format: 2000-01-01T00:00:00Z
		creation_date, _ = video.created_at.split('T', 1)

		if creation_date < config.vods_begin_date:
			break
		elif config.vods_begin_date <= creation_date <= config.vods_end_date:
			helix_video_list.append(video)

	helix_video_list = sorted(helix_video_list, key=lambda x: x.created_at)

	for video in helix_video_list:

		hours, minutes, seconds, _ = split_twitch_duration(video.duration)
		duration = f'{hours:02}:{minutes:02}:{seconds:02}'

		creation_datetime = convert_twitch_timestamp_to_datetime(video.created_at)
		end_datetime = creation_datetime + timedelta(hours=hours, minutes=minutes, seconds=seconds)
		
		creation_time = creation_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
		end_time = end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

		try:		
			db.execute(	'''
						INSERT OR IGNORE INTO Video (ChannelId, TwitchId, Title, CreationTime, Duration)
						VALUES (:channel_id, :twitch_id, :title, :creation_time, :duration);
						''',
						{'channel_id': config.channel_database_id, 'twitch_id': video.id, 'title': video.title, 'creation_time': creation_time, 'duration': duration})

		except sqlite3.Error as error:
			print(f'Could not insert the video {video.id} ({video.title}) with the error: {repr(error)}')

		# Sometimes Twitch errors can temporarily break the video's length and return longer duration (e.g. over 24 hours).
		# We'll query the duration again since that allows us to fix any wrong durations by editing the corresponding
		# column in the database. Since the previous query ignores duplicate VODs we won't overwrite our fix.
		# We'll also make sure to always validate the video ID associated with each chat message (e.g. if the wrong duration
		# is over 24 hours, then it might bleed into the next VOD).

		try:		
			cursor = db.execute('SELECT Duration FROM Video WHERE TwitchId = :twitch_id;', {'twitch_id': video.id})
			row = cursor.fetchone()
			duration = row['Duration']

			hours, minutes, seconds, _ = split_twitch_duration(duration)
			end_datetime = creation_datetime + timedelta(hours=hours, minutes=minutes, seconds=seconds)
			end_time = end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

		except sqlite3.Error as error:
			print(f'Could not retrieve the duration for the video {video.id} ({video.title}) with the error: {repr(error)}')

		try:
			db.execute(	'''
						UPDATE Chat SET VideoId = NULL
						WHERE VideoId = (SELECT Id FROM Video WHERE TwitchId = :twitch_id) AND Timestamp NOT BETWEEN :begin_time AND :end_time;
						''',
						{'twitch_id': video.id, 'begin_time': creation_time, 'end_time': end_time})

			db.execute(	'''
						UPDATE Chat SET VideoId = (SELECT Id FROM Video WHERE TwitchId = :twitch_id)
						WHERE VideoId IS NULL AND ChannelId = :channel_id AND Timestamp BETWEEN :begin_time AND :end_time;
						''',
						{'twitch_id': video.id, 'channel_id': config.channel_database_id, 'begin_time': creation_time, 'end_time': end_time})

		except sqlite3.Error as error:
			print(f'Could not update the chat for the video {video.id} ({video.title}) with the error: {repr(error)}')

	print(f'Found {len(helix_video_list)} videos in the "{config.vod_type}" section of the "{config.channel_name}" channel using the API.')
	print(f'The remaining API rate limit is {helix.api.rate_limit_remaining} of {helix.api.rate_limit_points} points.')
	print()

####################################################################################################

# Iterate over each VOD and its chat for the requested time period. For each VOD, we'll count the
# number of times a specific word or emote was sent in the chat, and also generate a plot with each
# category category's word frequency.

class Video():

	# From the database.
	Id: int
	ChannelId: int
	TwitchId: str
	Title: str
	CreationTime: str
	Duration: str
	YouTubeId: str
	Notes: str

	# Determined using the above.
	CreationDateTime: datetime
	CreationDate: str
	
	DurationInSeconds: int
	NumBuckets: int
	Frequency: dict
	
	HostId: str
	Url: str
	HasYouTubeUrl: bool

	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)

		self.CreationDateTime = datetime.fromisoformat(self.CreationTime)
		self.CreationDate = self.CreationDateTime.strftime('%Y-%m-%d')
		self.CreationTime = self.CreationDateTime.strftime('%Y-%m-%d %H:%M:%S')
			
		hours, minutes, seconds, duration_in_seconds = split_twitch_duration(self.Duration)
		self.Duration = f'{hours}h{minutes:02}m{seconds:02}s'
		self.DurationInSeconds = duration_in_seconds
		self.NumBuckets = ceil(duration_in_seconds / config.bucket_length)

		self.Frequency = {}
		for category in config.categories:
			self.Frequency[category.name] = [0] * self.NumBuckets

		if config.use_youtube_urls and self.YouTubeId is not None:
			self.HostId = self.YouTubeId
			self.Url = f'https://www.youtube.com/watch?v={self.YouTubeId}'
			self.HasYouTubeUrl = True
		else:
			self.HostId = self.TwitchId
			self.Url = f'https://www.twitch.tv/videos/{self.TwitchId}'
			self.HasYouTubeUrl = False

try:
	if config.vod_criteria == 'date':
		cursor = db.execute('SELECT * FROM Video WHERE ChannelId = :channel_id AND CreationTime BETWEEN :begin_time AND :end_time ORDER BY CreationTime;',
							{'channel_id': config.channel_database_id, 'begin_time': config.vods_begin_time, 'end_time': config.vods_end_time})
	elif config.vod_criteria == 'notes':
		cursor = db.execute('SELECT * FROM Video WHERE ChannelId = :channel_id AND Notes LIKE ("%" || :notes || "%") ORDER BY CreationTime;',
							{'channel_id': config.channel_database_id, 'notes': config.notes})
	
	video_list = [Video(**dict(row)) for row in cursor]
except sqlite3.Error as error:
	print(f'Failed to retrieve the videos {config.vods_criteria_text} with the error: {repr(error)}')
	sys.exit(1)

if not video_list:
	print(f'Could not find any videos in the "{config.channel_name}" channel {config.vods_criteria_text}.')
	sys.exit(1)

print(f'Found {len(video_list)} videos in the "{config.channel_name}" channel {config.vods_criteria_text}.')

for i, video in enumerate(video_list):

	print()
	print(f'- Processing the VOD {i+1} of {len(video_list)} "{video.Title}" ({video.HostId} at {video.CreationTime})...')

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
							''', {'video_id': video.Id})

	except sqlite3.Error as error:
		print(f'- Could not retrieve the chat with the error: {repr(error)}')
		continue

	for chat in cursor:

		if config.vod_criteria == 'date':
			assert config.vods_begin_time <= chat['Timestamp'] <= config.vods_end_time, 'The chat message was not sent during the live stream.'

		word_list = chat['Message'].lower().split()
		bucket = floor(chat['Offset'] / config.bucket_length)

		for category in config.categories:
			
			skip_to_next_category = False

			for word in word_list:
				
				for search_word in category.search_words:

					match = False
					if isinstance(search_word, str):
						match = (word == search_word)
					else:
						match = bool(search_word.match(word))

					if match:
						video.Frequency[category.name][bucket] += 1
						skip_to_next_category = True
						break					

				if skip_to_next_category:
					break

	if not config.plot_categories:
		continue

	# Plot the word frequency.
	figure, axis = plt.subplots(figsize=(12, 6))

	max_messages = 0
	for category in config.categories:

		if isinstance(config.plot_categories, list) and category.name not in config.plot_categories:
			continue

		y_data = video.Frequency[category.name]
		x_data = [i * config.bucket_length for i in range(len(y_data))]
		axis.plot(x_data, y_data, label=category.name, color=category.color, linewidth=0.7)

		max_messages = max(max_messages, max(y_data))

	if config.plot_threshold:
		axis.axhline(y=config.message_threshold, label=f'Threshold ({config.message_threshold})', color='k', linestyle='dashed')

	axis.set(xlabel=f'Time in Buckets of {config.bucket_length} Seconds', ylabel='Number of Messages', title=f'"{video.Title}" ({video.CreationTime}, {video.Duration})\n{video.Url}')
	axis.legend()
	
	# Format the number of seconds as 00h00.
	def seconds_formatter(num_seconds, position):
		label, _ = str(timedelta(seconds=num_seconds)).rsplit(':', 1)
		return label.replace(':', 'h', 1)

	axis.xaxis.set_major_formatter(seconds_formatter)
	axis.xaxis.set_major_locator(MultipleLocator(30*60 if video.DurationInSeconds >= 90*60 else 15*60))
	axis.xaxis.set_minor_locator(AutoMinorLocator(4))
	
	axis.yaxis.set_major_locator(MultipleLocator(5 if max_messages <= 100 else 10))

	axis.tick_params(axis='x', which='major', length=7)
	axis.tick_params(axis='x', which='minor', length=4)

	axis.set_ylim(-2)

	figure.tight_layout()

	plot_filename = f'{config.channel_name}_{video.CreationDate}_{video.HostId}.png'
	figure.savefig(plot_filename, dpi=200)
	print(f'- Saved the plot to "{plot_filename}".')

print()

if not config.plot_categories:
	print('- Skipped the chat message plots at the user\'s request.')
	print()

####################################################################################################

# Compare some of the previous categories against each other and compute the final balance. E.g. the number of +2 vs -2.

for video in video_list:
	for comparison in config.comparisons:

		def measure_controversy_1(positive_count: int, negative_count: int) -> float:
			return (positive_count + negative_count) / (abs(positive_count - negative_count) + 1)

		def measure_controversy_2(positive_count: int, negative_count: int) -> float:
			if positive_count == 0 or negative_count == 0:
				return 0
			else:
				return (positive_count + negative_count) ** (min(positive_count, negative_count) / max(positive_count, negative_count))

		positive_frequency = video.Frequency[comparison.positive_category.name]
		negative_frequency = video.Frequency[comparison.negative_category.name]

		video.Frequency[comparison.positive_name] = [positive_count - negative_count for positive_count, negative_count in zip(positive_frequency, negative_frequency)]
		video.Frequency[comparison.negative_name] = video.Frequency[comparison.positive_name].copy()
		video.Frequency[comparison.controversial_name] = [(measure_controversy_2(positive_count, negative_count), positive_count, negative_count) for positive_count, negative_count in zip(positive_frequency, negative_frequency)]
		
		# We'll use the comparison's name to count the total.
		video.Frequency[comparison.name] = [positive_count + negative_count for positive_count, negative_count in zip(positive_frequency, negative_frequency)]

for comparison in config.comparisons:
	for kind in ['positive', 'negative', 'controversial']:

		balance = CategoryBalance()

		balance.name = getattr(comparison, kind + '_name')
		balance.top = getattr(comparison, kind + '_top')
		balance.skip_summary = comparison.skip_summary
		balance.positive_words = comparison.positive_category.words
		balance.negative_words = comparison.negative_category.words
		balance.comparison_name = comparison.name
		balance.comparison_kind = kind

		config.categories.append(balance)

####################################################################################################

# Create a text file linking to the top highlights for each word and emote category.

print(f'Summarizing the top highlights.')
print()

summary_text = f'**Twitch Highlights ({config.vods_criteria_summary_title}):**\n\n'
summary_text += f'Counting the number of chat messages with specific words and emotes in a {config.bucket_length}-second window.\n\n&nbsp;\n\n'

Candidate = namedtuple('Candidate', ['Video', 'Bucket', 'Count'])

for category in config.categories:

	if category.skip_summary:
		continue
	
	is_balance = isinstance(category, CategoryBalance)

	highlight_candidates = []
	for video in video_list:
		
		# Filter buckets under a certain threshold. For category balance, we use the total number of cases (positive and negative).
		frequency = video.Frequency[category.name]
		total = frequency if not is_balance else video.Frequency[category.comparison_name]

		for i, count in enumerate(total):
			
			if count >= config.message_threshold:
				candidate = Candidate(video, i, frequency[i])
				highlight_candidates.append(candidate)
		
	# The controversial category balance, the frequency is a tuple with three elements: the controversy metric, the number of positive
	# messages, and the number of negative ones. This allows us to report the real values in the summary text formatting below, instead
	# of showing a potentially confusing metric.
	sort_key = (lambda x: x.Count[0]) if is_balance and category.comparison_kind == 'controversial' else (lambda x: x.Count)
	reverse_candidates = (not is_balance or category.comparison_kind != 'negative')
	highlight_candidates = sorted(highlight_candidates, key=sort_key, reverse=reverse_candidates)
	
	# Remove any candidates that occurred too close to each other, starting with the worst ones.
	# We don't have to do this step if we only want the best candidate, since that one is never
	# removed from the list.
	if config.top_bucket_distance_threshold is not None and category.top > 1:
		
		num_removed = 0
		for worse_idx, worse_candidate in reversed(list(enumerate(highlight_candidates))):
			for better_candidate in highlight_candidates:
				
				# Skip the same candidate (since they're the same) and any other future candidates
				# (since we already compared them in previous iterations of the outer loop).
				if worse_candidate is better_candidate:
					break

				worse_video_id = worse_candidate.Video.Id
				better_video_id = better_candidate.Video.Id

				if worse_video_id == better_video_id and abs(worse_candidate.Bucket - better_candidate.Bucket) < config.top_bucket_distance_threshold:
					# Remember that, since we're iterating backwards in the outer loop, we're removing
					# this element from the end of the list.
					del highlight_candidates[worse_idx]
					num_removed += 1
					break

		if num_removed > 0:
			print(f'- Removed {num_removed} "{category.name}" highlights that were fewer than {config.top_bucket_distance_threshold * config.bucket_length} seconds apart.')
	
	words_summary = ''
	if config.show_word_list:
		
		if is_balance:
			comparison_title = 'Highest' if category.comparison_kind == 'positive' else ('Lowest' if category.comparison_kind == 'negative' else category.comparison_kind.title())
			words_summary = f' ({comparison_title} Balance: ' + ', '.join(category.positive_words) + ' vs ' + ', '.join(category.negative_words) + ')'
		else:
			words_summary = ' (' + ', '.join(category.words) + ')'
	
	summary_text += f'**{category.name}**{words_summary}:\n\n'

	highlight_candidates = highlight_candidates[:category.top]

	if highlight_candidates:

		for i, candidate in enumerate(highlight_candidates):
			
			if is_balance:
				if category.comparison_kind == 'controversial':
					positive_count = candidate.Count[1]
					negative_count = candidate.Count[2]
					count = f'{positive_count} vs {negative_count}'
				else:
					count = f'{candidate.Count:+d}'
			else:
				count = str(candidate.Count)

			weekday = candidate.Video.CreationDateTime.strftime('%a (%d/%m)')

			# VOD timestamp format: 00h00m00s (Twitch), Number Of Seconds (YouTube)
			timestamp = timedelta(seconds=candidate.Bucket * config.bucket_length) - timedelta(seconds=config.top_url_delay)
			if timestamp.total_seconds() < 0:
				timestamp = timedelta(seconds=0)

			url_timestamp: Union[int, str]
			if candidate.Video.HasYouTubeUrl:
				url_timestamp = int(timestamp.total_seconds())
				highlight_url = f'{candidate.Video.Url}&t={url_timestamp}s'
			else:
				url_timestamp = str(timestamp).replace(':', 'h', 1).replace(':', 'm', 1) + 's'
				highlight_url = f'{candidate.Video.Url}?t={url_timestamp}'

			summary_text += f'{i+1}. [{count}] {weekday}: [REPLACEME]({highlight_url})\n\n'

	else:
		summary_text += f'- No highlights found.\n\n'

	print(f'- Found {len(highlight_candidates)} "{category.name}" highlights.')

print()

summary_text = summary_text.rstrip()

summary_filename = f'{config.channel_name}_{config.vods_criteria_filename_suffix}.txt'
with open(summary_filename, 'w', encoding='utf-8') as file:
	file.write(summary_text)

print(f'Saved the summary to "{summary_filename}".')

####################################################################################################

print()
print('Finished running.')