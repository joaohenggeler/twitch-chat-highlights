#!/usr/bin/env python3

"""
	Joins a given number of Twitch channels and saves any public chat messages to disk.
"""

import json
import os
import sqlite3

from twitchio.ext import commands

from keep_alive import keep_alive

####################################################################################################

with open('config.json') as file:
	CONFIG = json.load(file)

if 'access_token' not in CONFIG:
	
	if 'access_token' in os.environ:
		CONFIG['access_token'] = os.environ['access_token']
	else:
		CONFIG['access_token'] = input('Access Token: ')

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

			print(f'Connected to the database: ' + CONFIG['database_filename'])
		except sqlite3.Error as error:
			print(f'Failed to create or open the database with the error: {repr(error)}')

		try:
			for channel in CONFIG['bot']['channels']:
				self.db.execute('INSERT OR IGNORE INTO Channel (Name) VALUES (:channel);', {'channel': channel.lower()})
		except sqlite3.Error as error:
			print(f'Failed to insert the channel names with the error: {repr(error)}')

	async def event_ready(self):
		print(f'Logged in as "{self.nick}" to the channels: ' + str(CONFIG['bot']['channels']))

	async def event_token_expired(self):
		print('Attempting to renew the expired access token')
		return None

	async def event_error(self, error):
		print(f'Error: {repr(error)}')

	async def event_message(self, message):		
		if message.echo:
			return
		
		timestamp = message.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
		params = {'timestamp': timestamp, 'message': message.content, 'channel_name': message.channel.name}

		try:
			self.db.execute('''
							INSERT INTO Chat (Timestamp, Message, ChannelId)
							VALUES (:timestamp, :message, (SELECT CL.Id FROM Channel CL WHERE CL.Name = :channel_name));
							''', params)
		except sqlite3.Error as error:
			print(f'Failed to insert the message ({message.channel.name}, {timestamp}, "{message.content}") with the error: {repr(error)}')

	async def close(self):
		print(f'Logging off')

		try:
			self.db.close()
		except sqlite3.Error as error:
			print(f'Failed to close the database with the error: {repr(error)}')

####################################################################################################

if CONFIG['bot'].get('keep_alive'):
	print('Running keep-alive')
	keep_alive()

print('Starting the Chat Transcript Bot')

bot = ChatTranscriptBot()
bot.run()

print('Stopped the Chat Transcript Bot')