import ujson
import pandas as pd

user_ids = []
user_name = []
user_review_cnt = []
user_yelp_since = []
user_thumbup = []
user_friend_cnt = []
user_friend_list = []

with open('/Users/roxanne/Desktop/yelp_dataset/jsons/user.json', 'r') as f:
    user = {}
    for line in f:
        tmp = ujson.loads(line)
        tmp_user_id = tmp["user_id"]
        user[tmp_user_id] = tmp

        user_ids.append(tmp['user_id'])
        user_name.append(tmp['name'])
        user_review_cnt.append(tmp['review_count'])
        user_yelp_since.append(tmp['yelping_since'])
        user_thumbup.append(tmp['useful'] + tmp['funny'] + tmp['cool'])
        user_friend_cnt.append(len(tmp['friends']))
        user_friend_list.append(tmp['friends'])

data = {'user_ids': user_ids,
        'user_name': user_name,
        'user_review_cnt': user_review_cnt,
        'user_yelp_since': user_yelp_since,
        'user_thumbup': user_thumbup,
        'user_friend_cnt': user_friend_cnt
        }
df = pd.DataFrame.from_dict(data)
df.to_csv("/Users/roxanne/Desktop/data/clean_user.csv")

data2 = {'user_friend_list': user_friend_list}
df2 = pd.DataFrame.from_dict(data2)
df2.to_csv("/Users/roxanne/Desktop/data/user_friends.csv")