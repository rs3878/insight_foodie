import ujson
import pandas as pd

business_id = []
timestamp = []

with open('/Users/roxanne/Desktop/yelp_dataset/jsons/review.json', 'r') as f:
    checkin = {}
    for line in f:
        tmp = ujson.loads(line)
        tmp_business_id = tmp["business_id"]
        checkin[tmp_business_id] = tmp

        for time in tmp['date'].split(", "):
            business_id.append(tmp['business_id'])
            timestamp.append(time)

data = {'business_id':business_id,'time':timestamp}
df = pd.DataFrame.from_dict(data)
df.to_csv("/Users/roxanne/Desktop/data/clean_checkin.csv")