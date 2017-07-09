from datetime import datetime
import os
import random
import uuid
import pytest
import sqlite3
import os
import pandas as pd

cnx = sqlite3.connect(os.path.join(os.path.dirname('__file__'), 'tweets_lse.db'))

#  query sqlite table
lse_df = pd.read_sql_query('SELECT * FROM tweets', cnx)

content = list(lse_df['text'])


def get_content_data(**kwargs):

    attribs = {
        "language_content": content,
        "content_source": random.randint(1, 2),
        "content_handle": uuid.uuid4().hex,
        "content_date": datetime.now().isoformat(),
        "recipient_id": None,
        "content_tags": ['tag1', 'tag2', 'tag3'],
        'language': 'english'
    }
    attribs.update(kwargs)
    return attribs


def get_person_data(content=None):
    person_data = {'name': "John {0} Doe".format(uuid.uuid4().hex), 'person_handle': uuid.uuid4().hex, 'gender': 1}
    if content:
        person_data["content"] = content
    return person_data


def get_sample_csv_file(file_name):
    current_dir = os.path.dirname(os.path.realpath(__file__))

    test_file = os.path.join(current_dir, file_name)
    return test_file
