#!/usr/bin/env python3

"""
	A module that defines any general purpose functions used by all scripts, including loading configuration files,
	connecting to the database, and handling Twitch's timestamp formats.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Optional, Tuple, Union

####################################################################################################

class CommonConfig():

	# From the config file.
	json_config: dict

	client_id: str
	client_secret: Optional[str]
	access_token: str
	database_path: str

	def __init__(self):
		
		with open('config.json', encoding='utf-8') as file:
			self.json_config = json.load(file)
		
		self.__dict__.update(self.json_config['common'])

		self.database_path = os.path.abspath(self.database_path)

	def connect_to_database(self) -> sqlite3.Connection:

		os.makedirs(os.path.dirname(self.database_path), exist_ok=True)

		db = sqlite3.connect(self.database_path, isolation_level=None)
		db.row_factory = sqlite3.Row

		db.execute('''PRAGMA journal_mode = WAL;''')
		db.execute('''PRAGMA synchronous = NORMAL;''')
		db.execute('''PRAGMA temp_store = MEMORY;''')

		db.execute('''
						CREATE TABLE IF NOT EXISTS Channel
						(
						Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
						Name VARCHAR(50) NOT NULL UNIQUE
						);
						''')

		db.execute('''
						CREATE TABLE IF NOT EXISTS Video
						(
						Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
						ChannelId INTEGER NOT NULL,
						TwitchId VARCHAR(50) NOT NULL UNIQUE,
						Title TEXT NOT NULL,
						CreationTime TIMESTAMP NOT NULL,
						Duration TIME NOT NULL,
						YouTubeId VARCHAR(50) UNIQUE,
						Notes TEXT,

						FOREIGN KEY (ChannelId) REFERENCES Channel (Id)
						);
						''')

		# VideoId can be NULL when we're storing messages from a live stream, meaning there's no VOD yet.
		db.execute('''
						CREATE TABLE IF NOT EXISTS Chat
						(
						Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
						ChannelId INTEGER NOT NULL,
						VideoId INTEGER,
						Timestamp TIMESTAMP NOT NULL,
						Message TEXT NOT NULL,

						FOREIGN KEY (ChannelId) REFERENCES Channel (Id),
						FOREIGN KEY (VideoId) REFERENCES Video (Id)
						);
						''')

		return db

####################################################################################################

def split_twitch_duration(duration: str) -> Tuple[int, int, int, int]:

	# Duration format: 00h00m00s or 00m00s
	duration = duration.replace('h', ':').replace('m', ':').replace('s', '')
	tokens =  duration.split(':', 2)

	hours = int(tokens[-3]) if len(tokens) >= 3 else 0
	minutes = int(tokens[-2]) if len(tokens) >= 2 else 0
	seconds = int(tokens[-1]) if len(tokens) >= 1 else 0
	total_seconds = hours * 3600 + minutes * 60 + seconds
	
	return hours, minutes, seconds, total_seconds

def convert_twitch_timestamp_to_datetime(timestamp: str) -> datetime:

	# Datetime format: YYYY-MM-DDThh:mm:ss.sssZ
	# Where the following precisions where observed:
	# - YYYY-MM-DDThh:mm:ss.sssssssssZ
	# - YYYY-MM-DDThh:mm:ss.ssZ
	# - YYYY-MM-DDThh:mm:ss.sZ
	# - YYYY-MM-DDThh:mm:ssZ
	
	# Truncate anything past the microsecond precision.
	if '.' in timestamp:
		microseconds: Union[str, int]
		beginning, microseconds = timestamp.rsplit('.', 1)
		microseconds, _ = microseconds.rsplit('Z', 1)
		timestamp = beginning + '.' + microseconds[:6].ljust(6, '0') + 'Z'
	
	timestamp = timestamp.replace('Z', '+00:00')
	return datetime.fromisoformat(timestamp)