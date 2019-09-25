import ujson
import pandas as pd

review_ids = []
user_ids = []
business_ids = []
review_stars = []
review_thumbups = []
review_text = []
review_date = []

with open('/Users/roxanne/Desktop/yelp_dataset/jsons/review.json', 'r') as f:
    review = {}
    for line in f:
        tmp = ujson.loads(line)
        tmp_review_id = tmp["review_id"]
        review[tmp_review_id] = tmp

        review_ids.append(tmp['review_id'])
        user_ids.append(tmp['user_id'])
        business_ids.append(tmp['business_id'])
        review_stars.append(tmp['stars'])
        review_thumbups.append(tmp['useful']+tmp['cool']+tmp['funny'])
        #review_text.append(tmp['text'])
        review_date.append(tmp['date'])

data = {'user_ids': user_ids,
        'review_ids': review_ids,
        'business_ids': business_ids,
        'review_stars': review_stars,
        'review_thumbups': review_thumbups,
        #'review_text': review_text,
        'review_date': review_date
        }
df = pd.DataFrame.from_dict(data)
df.to_csv("/Users/roxanne/Desktop/data/clean_review.csv")