#!/usr/bin/env python3

"""
	Runs a bot that joins a given number of Twitch channels and saves any public chat messages to disk.
"""

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

from twitchio.ext import commands

####################################################################################################

with open('config.json') as file:
	CONFIG = json.load(file)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

current_timestamp = datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')
log_file_handler = logging.FileHandler(f'{current_timestamp}.log', 'w', 'utf-8')
log_stream_handler = logging.StreamHandler()

log_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_file_handler.setFormatter(log_formatter)
log_stream_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_stream_handler)

class ChatTranscriptBot(commands.Bot):

	def __init__(self):
		super().__init__(token=CONFIG['access_token'], prefix='!', initial_channels=CONFIG['bot']['channels'])

		try:
			self.db = sqlite3.connect(CONFIG['database_filename'])
			self.db.isolation_level = None

			self.db.execute('''
							CREATE TABLE IF NOT EXISTS 'Channel'
							(
							'Id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
							'Name' VARCHAR(50) NOT NULL UNIQUE
							);
							''')

			self.db.execute('''
							CREATE TABLE IF NOT EXISTS 'Video'
							(
							'Id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
							'ChannelId' INTEGER NOT NULL,
							'TwitchId' VARCHAR(50) NOT NULL UNIQUE,
							'Title' TEXT NOT NULL,
							'CreationTime' TIMESTAMP NOT NULL,
							'Duration' TIME NOT NULL,

							FOREIGN KEY (ChannelId) REFERENCES Channel (Id)
							);
							''')

			self.db.execute('''
							CREATE TABLE IF NOT EXISTS 'Chat'
							(
							'Id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
							'ChannelId' INTEGER NOT NULL,
							'VideoId' INTEGER,
							'Timestamp' TIMESTAMP NOT NULL,
							'Message' TEXT NOT NULL,

							FOREIGN KEY (ChannelId) REFERENCES Channel (Id),
							FOREIGN KEY (VideoId) REFERENCES Video (Id)
							);
							''')

			log.info(f'Connected to the database: ' + CONFIG['database_filename'])
		except sqlite3.Error as error:
			log.error(f'Failed to create or open the database with the error: {repr(error)}')

		self.message_tally = {}

		try:
			for channel in CONFIG['bot']['channels']:
				self.db.execute('INSERT OR IGNORE INTO Channel (Name) VALUES (:channel);', {'channel': channel.lower()})
				self.message_tally[channel] = {'success': 0, 'failure': 0, 'total': 0}
		except sqlite3.Error as error:
			log.error(f'Failed to insert the channel names with the error: {repr(error)}')

	async def event_ready(self):
		log.info(f'Logged in as "{self.nick}" to the channels: ' + str(CONFIG['bot']['channels']))

	async def event_token_expired(self):
		log.info('Attempting to renew the expired access token')
		return None

	async def event_error(self, error):
		log.error(f'Bot error: {repr(error)}')

	async def event_message(self, message):		
		if message.echo:
			return
		
		timestamp = message.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
		channel = message.channel.name.lower()
		params = {'timestamp': timestamp, 'message': message.content, 'channel': channel}

		for i in range(CONFIG['bot']['max_write_retries']):
			
			try:
				self.db.execute('''
								INSERT INTO Chat (Timestamp, Message, ChannelId)
								VALUES (:timestamp, :message, (SELECT CL.Id FROM Channel CL WHERE CL.Name = :channel));
								''', params)
			except sqlite3.Error as error:
				log.warning(f'Attempting to reinsert the message ({channel}, {timestamp}, "{message.content}") that failed with the error: {repr(error)}')
				await asyncio.sleep(CONFIG['bot']['write_retry_wait_time'])
			finally:
				self.message_tally[channel]['success'] += 1
				break

		else:
			log.error(f'Failed to insert the message ({channel}, {timestamp}, "{message.content}")')
			self.message_tally[channel]['failure'] += 1

		self.message_tally[channel]['total'] += 1

	async def close(self):
		try:
			self.db.close()
		except sqlite3.Error as error:
			log.warning(f'Failed to close the database with the error: {repr(error)}')

		log.info(f'Logged off "{self.nick}" from the channels with the following results: {self.message_tally}')

####################################################################################################

log.info('Starting the Chat Transcript Bot')

bot = ChatTranscriptBot()
bot.run()

log.info('Stopped the Chat Transcript Bot')