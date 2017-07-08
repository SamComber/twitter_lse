from io import StringIO
import json
import os
import tempfile
from expects import expect, equal
import pytest
from requests import post, get
import time
import conftest
from factories import get_sample_csv_file


@pytest.mark.upload_api
def test_upload_csv_with_email_notification_enabled(baseurl, apikey, apisecret):
    csv_file_to_upload = get_sample_csv_file("CSV_Upload_samples.csv")
    headers = conftest.auth_headers(apikey, apisecret)
    send_email = False

    response = upload_csv_data(conftest.upload_api_url(baseurl), headers, csv_file_to_upload, send_email)
    expect(response.status_code).to(equal(200))

    status_check_url = "{}{}".format(baseurl, response.json()["_links"]["self"]["href"])
    latest_response = response.json()
    idx = 1
    while latest_response["status"] not in ["completed", "failed", "errored"] and idx < 50:
        time.sleep(5)
        response = get(status_check_url, headers=headers)
        expect(response.status_code).to(equal(200))
        latest_response = response.json()
        print("Retry {}: Status - {}. Last updated = {}".format(idx, latest_response["status"], latest_response["updated"]))
        idx += 1
    expect(latest_response["result"]["success"]).to(equal(5))


def upload_csv_data(url, headers, upload_file_path, send_email=False):
    files = {'file': open(upload_file_path,'rb')}
    other_data = {}
    if send_email:
        other_data = {'send_email': send_email}

    resp = post(
        url, files=files,
            data=other_data, headers=headers
    )
    return resp
