# Index
The recipes have been organized under python unit tests to make them easy to execute. 

Please refer the [Swagger Docs](https://app.receptiviti.com/api/spec) for the latest specs for all the APIs 

## test_person_api.py 
This test covers the following scenarios for the [People API](https://app.receptiviti.com/api/spec#!/People) 
- Creating a person with content
- Creating a person without any content
- Adding content to an existing person
- Get all people in the system

## test_upload_api.py
This test cover the following scenarios of the [Upload API](https://app.receptiviti.com/api/spec#!/Upload_API)
- Uploading a CSV through the API
- Checking the status of the response
- Reviewing the import results

## test_twitter_import_api.py
This test covers the following scenarios of the [Twitter Import API](https://app.receptiviti.com/api/spec#!/Twitter_Import_API)
- Creating a request for a twitter handle to be imported
- Checking the status of the response
- Reviewing the import results
