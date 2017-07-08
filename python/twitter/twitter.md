##Twitter API

All requests must contain the authentication keys and the content type must be set to application/json:-

```
X-API-KEY: <your api key>
X-API-SECRET-KEY: <your api secret key>
Content-Type: application/json; charset=utf-8
```

There are three types of Twitter requests - user, followers and hashtag. This document covers only the first.


### user: analyse the tweets of a single user

The payload must contain a json object with a single field called ```screen_name```. Its value is the handle of the user to analyse. In addition remember to set your credentials in the header and set the *Content-Type* to *application/json*.

#####Sample request:

```
POST /api/import/twitter/user HTTP/1.1
Accept: application/json
Accept-Encoding: gzip, deflate, compress
Content-Length: 29
Content-Type: application/json; charset=utf-8
Host: 10.0.2.15
User-Agent: HTTPie/0.8.0
X-API-KEY: <your api key>
X-API-SECRET-KEY: <your api secret key>

{
    "screen_name": "anncoulter"
}
```

A sample response is shown below. The 3 important fields are *status*, *_links.people.href* and *_links.self.href*.

- *_links.self.href* is the url to use to get the status of this request
- *_links.people.href* is the url to use to get the result of this request, **after** status has been set to *finished*
- *status* shows the status of the request and it will usually be set *processing*, until the system has completed extracting and analysing the data from Twitter. See the section **Request status** for more information.

#####Sample response:

```
HTTP/1.1 200 OK
Connection: keep-alive
Content-Length: 693
Content-Type: application/hal+json
Date: Wed, 01 Jun 2016 14:14:15 GMT
Server: nginx/1.10.0

{
    "_id": "574eedb7b0f6f40a5cd4b245", 
    "_links": {
        "people": {
            "href": "/api/import/twitter/requests/574eedb7b0f6f40a5cd4b245/people", 
            "method": "GET"
        }, 
        "self": {
            "href": "/api/import/twitter/requests/574eedb7b0f6f40a5cd4b245", 
            "method": "GET"
        }
    }, 
    "created": "2016-06-01T14:14:15.874520+00:00", 
    "error_message": null, 
    "falcon_response": {
        "_id": "574eedb7b0f6f4062f61a3a7", 
        "handles_count": 0, 
        "is_done": false, 
        "search_key": "anncoulter", 
        "status": "new", 
        "total_handles": 1
    }, 
    "operation": "Individual", 
    "request_url_root": "http://10.0.2.15/", 
    "search_key": "anncoulter", 
    "status": "Processing", 
    "updated": "2016-06-01T14:14:15.874520+00:00", 
    "upload_errors": [], 
    "user": "574de846b0f6f408b01788c9"
}
```


###Request status

This endpoint is used to get the status of a submitted Twitter request. After submitting the request to import a user from Twitter, use this endpoint to check for the status. Note that the form of the request is /api/import.twitter/requests/<id> where <id> is the value of the *_id* field returned in the original request. You can simply issue a GET request using the value of the *_links.self.href' as the url, and setting your credentials.

#####Sample request:

```
GET /api/import/twitter/requests/574eedb7b0f6f40a5cd4b245 HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate, compress
Host: 10.0.2.15
User-Agent: HTTPie/0.8.0
X-API-KEY: <your api key>
X-API-SECRET-KEY: <your api secret key>
```

In the sample below, the *status* field is set to *Finished*. This means the twitter data has been gathered and analysysed, and that you can retrieve the results using the url in the *_links.people.href* field

#####Sample response:

```
HTTP/1.1 200 OK
Connection: keep-alive
Content-Length: 876
Content-Type: application/hal+json
Date: Wed, 01 Jun 2016 14:16:18 GMT
Server: nginx/1.10.0

{
    "_id": "574eedb7b0f6f40a5cd4b245", 
    "_links": {
        "people": {
            "href": "/api/import/twitter/requests/574eedb7b0f6f40a5cd4b245/people", 
            "method": "GET"
        }, 
        "self": {
            "href": "/api/import/twitter/requests/574eedb7b0f6f40a5cd4b245", 
            "method": "GET"
        }
    }, 
    "created": "2016-06-01T14:14:15.874000+00:00", 
    "dataset_id": "574eedbeb0f6f4087b7ae18d", 
    "error_message": null, 
    "falcon_response": {
        "_id": "574eedb7b0f6f4062f61a3a7", 
        "handles_count": 1, 
        "is_done": true, 
        "remote_file_location": "twitter/7c371f00aa26425b8b6531fec50cb95f_574eedb7b0f6f4062f61a3a7.csv", 
        "search_key": "anncoulter", 
        "status": "done", 
        "total_handles": 1
    }, 
    "operation": "Individual", 
    "request_url_root": "http://10.0.2.15/", 
    "search_key": "anncoulter", 
    "status": "Finished", 
    "updated": "2016-06-01T14:14:21.379000+00:00", 
    "upload_errors": [], 
    "upload_request": "574eedbdb0f6f4087b7ae18c", 
    "user": "574de846b0f6f408b01788c9"
}
```

###Result

You can retrieve the analysis of the person by issuing a GET request on the url in *_links.people.href*. This is the output obtained by the submitting the tweets of the selected use to the Receptiviti language engine. Remember to include your credentials in the request header.

#####Sample request:

```
GET /api/import/twitter/requests/574eedb7b0f6f40a5cd4b245/people HTTP/1.1
Accept: */*
Accept-Encoding: gzip, deflate, compress
Host: 10.0.2.15
User-Agent: HTTPie/0.8.0
X-API-KEY: <your api key>
X-API-SECRET-KEY: <your api secret key>
```

#####Sample response:

```
HTTP/1.1 200 OK
Connection: keep-alive
Content-Length: 6787
Content-Type: application/hal+json
Date: Wed, 01 Jun 2016 14:23:54 GMT
Server: nginx/1.10.0

[
    {
        "_id": "574eedbeb0f6f4087b7ae18e", 
        "content_count": 1, 
        "created": "2016-06-01T14:14:22.021000+00:00", 
        "custom_fields": [], 
        "datasets": [
            "574eedbeb0f6f4087b7ae18d"
        ], 
        "gender": 0, 
        "liwc_scores": {
            "analysis": {
                "analytic": 65.95937, 
                "authentic": 18.2699, 
                "categories": {
                    "AllPunc": 0.2867215, 
                    "Apostro": 0.04876616, 
                    "Colon": 0.024089307, 
                    "Comma": 0.038190365, 
                    "Dash": 0.0052878964, 
                    "Exclam": 0.022326674, 
                    "OtherP": 0.012338425, 
                    "Parenth": 0.0011750881, 
                    "Period": 0.06815511, 
                    "QMark": 0.016451234, 
                    "Quote": 0.04347826, 
                    "SemiC": 0.0064629847, 
                    "achieve": 0.011750882, 
                    "adj": 0.035840187, 
                    "adverb": 0.049941245, 
                    "affect": 0.07755582, 
                    "affiliation": 0.013513514, 
                    "anger": 0.026439482, 
                    "anx": 0.00058754405, 
                    "article": 0.045828436, 
                    "assent": 0.0029377204, 
                    "auxverb": 0.09576968, 
                    "bio": 0.020564042, 
                    "body": 0.0029377204, 
                    "cause": 0.018213866, 
                    "certain": 0.019976499, 
                    "cogproc": 0.1239718, 
                    "compare": 0.015276146, 
                    "conj": 0.03936545, 
                    "death": 0.0047003524, 
                    "differ": 0.032314923, 
                    "discrep": 0.025264395, 
                    "drives": 0.07109283, 
                    "family": 0.0017626322, 
                    "feel": 0.0070505287, 
                    "female": 0.0064629847, 
                    "filler": 0.0, 
                    "focusfuture": 0.013513514, 
                    "focuspast": 0.034077555, 
                    "focuspresent": 0.09576968, 
                    "friend": 0.0052878964, 
                    "function": 0.40893066, 
                    "health": 0.0029377204, 
                    "hear": 0.008813161, 
                    "home": 0.0023501762, 
                    "i": 0.022914218, 
                    "informal": 0.014101057, 
                    "ingest": 0.0, 
                    "insight": 0.021151586, 
                    "interrog": 0.02173913, 
                    "ipron": 0.044065803, 
                    "leisure": 0.005875441, 
                    "male": 0.016451234, 
                    "money": 0.011750882, 
                    "motion": 0.014101057, 
                    "negate": 0.022326674, 
                    "negemo": 0.045240894, 
                    "netspeak": 0.0064629847, 
                    "nonflu": 0.0035252643, 
                    "number": 0.017626323, 
                    "percept": 0.025264395, 
                    "posemo": 0.031139836, 
                    "power": 0.035252646, 
                    "ppron": 0.055816688, 
                    "prep": 0.11398355, 
                    "pronoun": 0.09988249, 
                    "quant": 0.013513514, 
                    "relativ": 0.11692127, 
                    "relig": 0.0029377204, 
                    "reward": 0.012925969, 
                    "risk": 0.0064629847, 
                    "sad": 0.0047003524, 
                    "see": 0.008813161, 
                    "sexual": 0.014688602, 
                    "shehe": 0.012338425, 
                    "social": 0.08343126, 
                    "space": 0.058754407, 
                    "swear": 0.00058754405, 
                    "tentat": 0.023501763, 
                    "they": 0.0052878964, 
                    "time": 0.04465335, 
                    "verb": 0.1639248, 
                    "we": 0.0070505287, 
                    "work": 0.025264395, 
                    "you": 0.008225617
                }, 
                "clout": 58.140015, 
                "dic": 0.71915394, 
                "sixLtr": 0.21621622, 
                "tone": 8.780424, 
                "wc": 1702, 
                "wps": 10.980645
            }, 
            "rawData": {
                "categories": {
                    "1": 696, 
                    "10": 78, 
                    "100": 199, 
                    "101": 24, 
                    "102": 100, 
                    "103": 76, 
                    "11": 194, 
                    "110": 43, 
                    "111": 10, 
                    "112": 4, 
                    "113": 20, 
                    "114": 5, 
                    "115": 8, 
                    "12": 163, 
                    "120": 24, 
                    "121": 1, 
                    "122": 11, 
                    "123": 5, 
                    "124": 6, 
                    "13": 85, 
                    "14": 67, 
                    "15": 38, 
                    "2": 170, 
                    "20": 279, 
                    "21": 61, 
                    "22": 26, 
                    "23": 37, 
                    "24": 30, 
                    "25": 23, 
                    "2600": 116, 
                    "2601": 65, 
                    "2602": 41, 
                    "2603": 11, 
                    "2604": 28, 
                    "2605": 38, 
                    "2606": 9, 
                    "2607": 74, 
                    "2608": 83, 
                    "2609": 2, 
                    "2610": 21, 
                    "3": 95, 
                    "30": 132, 
                    "31": 53, 
                    "32": 77, 
                    "33": 1, 
                    "34": 45, 
                    "35": 8, 
                    "4": 39, 
                    "40": 142, 
                    "41": 3, 
                    "42": 9, 
                    "43": 11, 
                    "44": 28, 
                    "5": 12, 
                    "50": 211, 
                    "51": 36, 
                    "52": 31, 
                    "53": 43, 
                    "54": 40, 
                    "55": 34, 
                    "56": 55, 
                    "6": 14, 
                    "60": 43, 
                    "61": 15, 
                    "62": 15, 
                    "63": 12, 
                    "7": 21, 
                    "70": 35, 
                    "71": 5, 
                    "72": 5, 
                    "73": 25, 
                    "8": 9, 
                    "80": 121, 
                    "81": 23, 
                    "82": 20, 
                    "83": 60, 
                    "84": 22, 
                    "85": 11, 
                    "9": 75, 
                    "90": 58, 
                    "91": 163, 
                    "92": 23
                }, 
                "dictionaryWordsCount": 1224, 
                "numbersCount": 22, 
                "punctuationMarksCount": 488, 
                "sentencesCount": 155, 
                "wordsCount": 1702, 
                "wordsLongerThan6LettersCount": 368
            }
        }, 
        "name": "anncoulter", 
        "person_handle": "twitter/anncoulter", 
        "person_tags": [
            "twitter", 
            "twitter/anncoulter", 
            "twitter/anncoulter"
        ], 
        "personality_snapshot": [
            {
                "description": "Tends to be hard driving, competitive, with a sense of time urgency. They can also show signs of hostility when threatened.", 
                "summary": "Intense drive."
            }, 
            {
                "description": "Is strongly focused on sexual themes, concepts, or ideas. Spends time considering sex and is likely comfortable discussing sex.", 
                "summary": "Focused on sexual topics."
            }, 
            {
                "description": "Likely often seen as critical, uncooperative, and even rude. Is likely a straight shooter and speaks their mind.", 
                "summary": "Disagreeable."
            }
        ], 
        "receptiviti_scores": {
            "category_ratings": {
                "achievement_driven": 1, 
                "adjustment": 3, 
                "agreeable": 1, 
                "body_focus": 1, 
                "cold": 5, 
                "conscientious": 2, 
                "depression": 2, 
                "extraversion": 1, 
                "family_oriented": 2, 
                "food_focus": 1, 
                "friend_focus": 2, 
                "happiness": 1, 
                "health_oriented": 1, 
                "impulsive": 5, 
                "independent": 4, 
                "insecure": 5, 
                "leisure_oriented": 1, 
                "money_oriented": 3, 
                "netspeak_focus": 1, 
                "neuroticism": 4, 
                "openness": 5, 
                "persuasive": 2, 
                "power_driven": 3, 
                "religion_oriented": 1, 
                "reward_bias": 1, 
                "sexual_focus": 5, 
                "social_skills": 1, 
                "thinking_style": 4, 
                "type_a": 5, 
                "work_oriented": 5, 
                "workhorse": 1
            }, 
            "percentiles": {
                "achievement_driven": 1.4298088682611167, 
                "adjustment": 44.99927624414327, 
                "agreeable": 1, 
                "body_focus": 15.848779807762861, 
                "cold": 91.89840258299421, 
                "conscientious": 29.902888277497908, 
                "depression": 30.594533973067087, 
                "extraversion": 1.937724772952844, 
                "family_oriented": 31.103126424324866, 
                "food_focus": 15.079053843759723, 
                "friend_focus": 53.15587156673114, 
                "happiness": 1.3545079253675296, 
                "health_oriented": 17.736738276738276, 
                "impulsive": 96.00097745478124, 
                "independent": 76.38680587334746, 
                "insecure": 82.35906432878673, 
                "leisure_oriented": 13.56621085830142, 
                "money_oriented": 92.52913116559439, 
                "netspeak_focus": 22.92951412429379, 
                "neuroticism": 61.71979407672427, 
                "openness": 98.88320757776434, 
                "persuasive": 20.837205955921213, 
                "power_driven": 54.945911043026946, 
                "religion_oriented": 34.52859020310633, 
                "reward_bias": 25.856623594819162, 
                "sexual_focus": 99, 
                "social_skills": 3.76640488582499, 
                "thinking_style": 65.73829122899464, 
                "type_a": 99, 
                "work_oriented": 81.01293687007973, 
                "workhorse": 2.042576686941617
            }, 
            "raw_scores": {
                "achievement_driven": 0.8371165108467205, 
                "adjustment": -6.645615818421559, 
                "agreeable": -6.004689883884092, 
                "body_focus": 0.29377204, 
                "cold": 1.1674309758964394, 
                "conscientious": -0.006567842736194152, 
                "depression": 0.1331321939414083, 
                "extraversion": -0.10255464678644532, 
                "family_oriented": 2.567898878503214, 
                "food_focus": 0.0, 
                "friend_focus": 0.5287896400000001, 
                "happiness": 0.6530272561673334, 
                "health_oriented": 0.29377204, 
                "impulsive": 5.264643685469207, 
                "independent": -3.5990665033371894, 
                "insecure": 2.2889267860486036, 
                "leisure_oriented": 0.5875441, 
                "money_oriented": 1.1750882, 
                "netspeak_focus": 0.5875441, 
                "neuroticism": 7.446180996175708, 
                "openness": 4.030405487317342, 
                "persuasive": -3.007269116219113, 
                "power_driven": 5.668712531657439, 
                "religion_oriented": 0.29377204, 
                "reward_bias": 0.6462984300000001, 
                "sexual_focus": 1.4688602, 
                "social_skills": -0.3959053046387878, 
                "thinking_style": -0.8347165466955394, 
                "type_a": 11.683130106172687, 
                "work_oriented": 2.5, 
                "workhorse": -7.013310235525225
            }
        }, 
        "updated": "2016-06-01T14:14:22.021000+00:00", 
        "user": "574de846b0f6f408b01788c9"
    }
]
```
