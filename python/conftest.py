# -*- coding: utf-8 -*-

import pytest

content = '''
FEMALE MODELS NEEDED for NEXT WEEK - 11am to 3pm -TFP HAIR N MAKEUP on - mon 10th july, tues… https://t.co/1f66MQ4Weh
'''

def pytest_addoption(parser):
    parser.addoption("--baseurl", action="store", default="https://app.receptiviti.com", help="Specify this in the format https://<target_domain_name>")
    parser.addoption("--key", action="store", default=None, help="The API key for the test user")
    parser.addoption("--secret", action="store", default=None, help="The API secret for the test user")


@pytest.fixture
def baseurl(request):
    return request.config.getoption("--baseurl")


@pytest.fixture
def apikey(request):
    return request.config.getoption("--key")


@pytest.fixture
def apisecret(request):
    return request.config.getoption("--secret")


@pytest.fixture
def twitter_handle():
    return 'anncoulter'


def twitter_import_user_api_url(baseurl):
    return "{}/import/twitter/user".format(api_base_url(baseurl))


def api_base_url(baseurl):
    return "{}/v2/api".format(baseurl)


def person_api_url(baseurl):
    return "{}/person".format(api_base_url(baseurl))

def merge_personality_api_url(baseurl):
    return "{}/person/merged_personality".format(api_base_url(baseurl))


def person_content_api_url(baseurl, person_id):
    return "{}/person/{}/contents".format(api_base_url(baseurl), person_id)


def upload_api_url(baseurl):
    return "{}/upload/upload_request".format(api_base_url(baseurl))


def ping_url(baseurl):
    return "{}/ping".format(api_base_url(baseurl))


def base_headers(apikey, apisecret):
    header = auth_headers(apikey, apisecret)
    header['Content-type'] = 'application/json'
    return header


def auth_headers(apikey, apisecret):
    header = {}
    if apikey:
        header['X-API-KEY'] = apikey
    if apisecret:
        header['X-API-SECRET-KEY'] = apisecret
    return header
