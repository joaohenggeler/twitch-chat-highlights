#!/usr/bin/env python3

import asyncio
import logging
import sqlite3
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import List

from twitchio.ext import commands # type: ignore

from common import CommonConfig

class BotConfig(CommonConfig):

	# From the config file.
	channels: List[str]
	max_write_retries: int
	write_retry_wait_time: int

	def __init__(self):
		super().__init__()
		self.__dict__.update(self.json_config['bot'])

if __name__ == '__main__':

	parser = ArgumentParser(description='Runs a bot that joins a given number of Twitch channels and saves any public chat messages sent during a live stream to the database. Be sure to get a streamer\'s permission before running this bot on their channel.')
	args = parser.parse_args()

	config = BotConfig()

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
			super().__init__(token=config.access_token, prefix='!', initial_channels=config.channels)

			try:
				self.db = config.connect_to_database()
				log.info(f'Connected to the database: {config.database_path}')
			except sqlite3.Error as error:
				log.error(f'Failed to connect to the database with the error: {repr(error)}')

			self.message_tally = {}

			try:
				for channel_name in config.channels:
					self.db.execute('INSERT OR IGNORE INTO Channel (Name) VALUES (:name);', {'name': channel_name.lower()})
					self.message_tally[channel_name] = {'success': 0, 'failure': 0, 'total': 0}
			except sqlite3.Error as error:
				log.error(f'Failed to insert the channel names with the error: {repr(error)}')

		async def event_ready(self):
			log.info(f'Logged in as "{self.nick}" to the channels: ' + str(config.channels))

		async def event_token_expired(self):
			log.info('Attempting to renew the expired access token')
			return None

		async def event_error(self, error):
			log.error(f'Bot error: {repr(error)}')

		async def event_message(self, message):		
			if message.echo:
				return
			
			timestamp = message.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
			channel_name = message.channel.name.lower()

			for i in range(config.max_write_retries):
				
				try:
					self.db.execute('''
									INSERT INTO Chat (ChannelId, Timestamp, Message)
									VALUES ((SELECT CL.Id FROM Channel CL WHERE CL.Name = :channel_name), :timestamp, :message);
									''', {'channel_name': channel_name, 'timestamp': timestamp, 'message': message.content})
				except sqlite3.Error as error:
					log.warning(f'Attempting to reinsert the message ({channel_name}, {timestamp}, "{message.content}") that failed with the error: {repr(error)}')
					await asyncio.sleep(config.write_retry_wait_time)
				finally:
					self.message_tally[channel_name]['success'] += 1
					break

			else:
				log.error(f'Failed to insert the message ({channel_name}, {timestamp}, "{message.content}")')
				self.message_tally[channel_name]['failure'] += 1

			self.message_tally[channel_name]['total'] += 1

		async def close(self):
			try:
				self.db.close()
			except sqlite3.Error as error:
				log.warning(f'Failed to close the database with the error: {repr(error)}')

			log.info(f'Logged off "{self.nick}" from the channels with the following results: {self.message_tally}')

	log.info('Starting the Chat Transcript Bot')

	bot = ChatTranscriptBot()
	bot.run()

	log.info('Stopped the Chat Transcript Bot')