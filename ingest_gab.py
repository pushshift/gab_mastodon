#!/usr/bin/env python3

import ujson as json
import requests
import sqlite3
import time
import datetime
import sys
from bs4 import BeautifulSoup
import html
import logging
from collections import defaultdict


def insert_into_es(rows, index='gab', action='create'):

    records = []

    for record in rows:

        # Add text fields for HTML fields
        text = BeautifulSoup(html.unescape(record['content']), features="html.parser").get_text()
        record['body'] = text
        note_text = BeautifulSoup(html.unescape(record['account']['note'])).get_text()
        record['account']['note_text'] = note_text

        id = str(record['id'])
        bulk = defaultdict(dict)
        bulk[action]['_index'] = index
        bulk[action]['_id'] = id
        records.extend(list(map(lambda x: json.dumps(x), [bulk, record])))

    headers = {'Accept': 'application/json', 'Content-type': 'application/json'}
    url = "http://localhost:9200/_bulk"
    records = '\n'.join(records) + "\n"
    response = requests.post(url, data=records, headers=headers)
    if response.status_code != 200:
        sys.exit(response.text)


def insert_batch(rows):

    sql = "INSERT OR REPLACE INTO post VALUES {}"
    args_str = ','.join(["(?,?,?,?)" for x in rows])
    sql = sql.format(args_str)
    c.execute(sql, list(sum(rows, ())))
    conn.commit()


def get_min():

    c.execute("SELECT id FROM post ORDER BY id ASC limit 1")
    id = c.fetchone()
    if id is not None:
        id = int(id[0])
    return id


def get_max():

    c.execute("SELECT id FROM post ORDER BY id DESC limit 1")
    id = c.fetchone()
    if id is not None:
        id = int(id[0])
    return id


def fetch_posts(max_id):

    base_url = 'https://gab.com/api/v1/timelines/public'
    params = {'only_media': 'false', 'local': 'true', 'limit': 40, 'max_id': max_id}

    while True:
        r = requests.get(base_url, params=params)
        response_headers = r.headers
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            reset_time = datetime.datetime.strptime(response_headers['X-Ratelimit-Reset'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
            wait = (reset_time - time.time()) + 1
            print("Sleeping for {}".format(wait))
            print(response_headers)
            time.sleep(wait)
        elif str(r.status_code).startswith('5'):
            time.sleep(2)
        else:
            print(r.content)
            sys.exit("Something went wrong with the fetch.")


def fetch_current():

    breakpoint = get_max()
    max_current_id = None
    db_rows = []
    es_rows = []
    stop_flag = False
    created_at = None
    created_at_string = None

    while True:

        posts = fetch_posts(max_current_id)

        for post in posts:
            id = int(post['id'])
            if max_current_id is None or id < max_current_id:
                max_current_id = id
            if max_current_id < breakpoint:
                stop_flag = True
            created_at = datetime.datetime.strptime(post['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
            created_at_string = post['created_at']
            retrieved_utc = time.time()
            data = json.dumps(post, ensure_ascii=False, escape_forward_slashes=False, sort_keys=True)
            es_rows.append(post)
            db_rows.append((id, created_at, retrieved_utc, data))

        logging.info("Current position: {} ({})".format(max_current_id, created_at_string))

        if stop_flag:
            break

    insert_into_es(es_rows)
    insert_batch(db_rows)


SQLITE_DATABASE_FILE = '/pool1/gab/gab.db'

logging.basicConfig(level=logging.INFO)
conn = sqlite3.connect(SQLITE_DATABASE_FILE, timeout=60.0)
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS post (id INTEGER PRIMARY KEY, created_at REAL, retrieved_utc REAL, data BLOB)')
max_id = get_min()
counter = 0

while True:

    posts = fetch_posts(max_id)
    rows = []
    created_at = None
    created_at_string = None

    insert_into_es(posts)

    for post in posts:
        id = int(post['id'])
        if max_id is None or id < max_id:
            max_id = id
        created_at = datetime.datetime.strptime(post['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
        created_at_string = post['created_at']
        retrieved_utc = time.time()
        data = json.dumps(post, ensure_ascii=False, escape_forward_slashes=False, sort_keys=True)
        rows.append((id, created_at, retrieved_utc, data))

    insert_batch(rows)
    logging.info("Current position: {} ({})".format(max_id, created_at_string))
    time.sleep(1)
    counter += 1

    if counter % 30 == 0:
        fetch_current()
