#!/usr/bin/env python3

"""
	A simple web server that keeps the bot alive in Repl.
"""

from threading import Thread

from flask import Flask

app = Flask('')

@app.route('/')
def home():
	return "Hello. I am alive!"

def run():
	app.run(host='0.0.0.0',port=8080)

def keep_alive():
	t = Thread(target=run)
	t.start()