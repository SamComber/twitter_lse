#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2016 Receptiviti
#
# Author: Abdul Gani
#
#
import pytest
from requests import *
import conftest
from expects import expect, equal
import time


@pytest.mark.person_api
def test_twitter_profile(baseurl, apikey, apisecret, twitter_handle):
    headers = conftest.auth_headers(apikey, apisecret)

    response = import_twitter_user(conftest.twitter_import_user_api_url(baseurl), headers, twitter_handle)
    expect(response.status_code).to(equal(200))

    status_check_url = "{}{}".format(baseurl, response.json()["_links"]["self"]["href"])
    latest_response = response.json()
    idx = 1
    while latest_response["status"] not in ["Finished", "Failed", "Error"] and idx < 50:
        time.sleep(5)
        response = get(status_check_url, headers=headers)
        expect(response.status_code).to(equal(200))
        latest_response = response.json()
        print("Retry {}: Status - {}. Last updated = {}".format(idx, latest_response["status"], latest_response["updated"]))
        idx += 1
    expect(latest_response["search_key"]).to(equal(twitter_handle))
    expect(latest_response["status"]).to(equal('Finished'))

    people_url = "{}{}".format(baseurl, response.json()["_links"]["people"]["href"])
    response = get(people_url, headers=headers)
    expect(response.status_code).to(equal(200))


def import_twitter_user(url, headers, handle):
    print url
    print headers
    other_data = {'screen_name': handle}

    return  post(url, data=other_data, headers=headers, allow_redirects=False)
