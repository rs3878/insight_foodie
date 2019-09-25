import ujson
import pandas as pd

business_ids = []
business_names = []
business_address = []
business_city = []
business_state = []
business_postal_code = []
business_longitude = []
business_latitude = []
business_stars = []
business_review_cnt = []

with open('/Users/roxanne/Desktop/yelp_dataset/jsons/business.json', 'r') as f:
    business = {}
    for line in f:
        tmp_business = ujson.loads(line)
        try:
            if tmp_business['attributes'].get('RestaurantsTakeOut',None) or tmp_business['attributes'].get('RestaurantsPriceRange2',None) or tmp_business['attributes'].get('RestaurantsTableService', None):
                tmp_business_id = tmp_business["business_id"]
                business[tmp_business_id] = tmp_business
                
                business_ids.append(tmp_business["business_id"])
                business_names.append(tmp_business["name"])
                business_address.append(tmp_business["address"])
                business_city.append(tmp_business["city"])
                business_state.append(tmp_business["state"])
                business_postal_code.append(tmp_business["postal_code"])
                business_longitude.append(tmp_business["longitude"])
                business_latitude.append(tmp_business["latitude"])
                business_stars.append(tmp_business["stars"])
                business_review_cnt.append(tmp_business['review_count'])
        except:
            pass

data = {'business_ids': business_ids,
        'business_names': business_names,
        'business_address': business_address,
        'business_city': business_city,
        'business_state': business_state,
        'business_postal_code': business_postal_code,
        'business_longitude': business_longitude,
        'business_latitude': business_latitude,
        'business_stars': business_stars,
        'business_review_cnt': business_review_cnt
        }
df = pd.DataFrame.from_dict(data)

df.to_csv("/Users/roxanne/Desktop/data/clean_business.csv")
