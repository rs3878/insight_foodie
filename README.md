# datars
This app uses spark-collaborative filtering to generate user features based on the ratings and use clustering models to 
find users that have similar tastes.   
I will use the yelp challenge data set for building model and generate fake user check-in data for real-time streaming. 

### Step 1: 
- The python files transformed the json data to csv format with useful info selected.  
- While transforming the businesses data, it selected only restaurants out of all businesses.  
- The first sql command created the table used for collaborative filtering by selecting out following columns:  
user_id, business_id, rating, time.
- The second sql command selected for each user the city that has the most rated businesses as his/her primary city.   
### Step 2:
- Using spark collaborative filtering, the model generated user feature matrix, business feature matrix and 
predicted rating for each pair of user and business.  
- Apply spark K-means algorithm to cluster users and businesses respectively, assign each user and restaurant a type.
- Output user types, restaurant types and predictions of user-restaurants pair order by predicted rating.  
### Step 3:
- Streaming real-time check-in data (user_id, business_id, timestamp) using kafka and store it to postgres directly  
### Step 4:
- Use complex sql queries to fulfill front end needs
- When a user inputs his/her user_id, and the city he/she is currently in, he/she would be able to find foodies to eat 
with and see the restaurants those foodies has rate before.

