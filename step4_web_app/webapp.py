from flask import Flask, request, render_template, redirect
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from sqlalchemy import create_engine
import boto3

#2_JQ0h10-Aj_Bu9GbtzJ9Q
#Las Vegas
conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
cursor = conn.cursor()
engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')

business = sqlio.read_sql_query("""SELECT * FROM clean_business""", conn)
user = sqlio.read_sql_query("""SELECT * FROM clean_user""", conn)
consumer = pd.read_sql('SELECT * FROM consumer', engine)

app = Flask(__name__)

@app.route("/")
def checkingin():
	return render_template("page.html")

@app.route("/checkin", methods = ['GET'])
def checkin_info():
	return render_template("checkin.html")

@app.route("/result", methods = ['POST'])
def fcn():
	q = """SELECT prediction from kmeans_users where user_ids = '{}'"""
	print(request.form)
	u_id = request.form['uid']
	cursor.execute(q.format(u_id))
	prediction = cursor.fetchone()[0]
	u_city = request.form['city']
	q = """select distinct user_name, user_id \
			from consumer \
			where prediction = {} and city='{}' and time >= NOW() - INTERVAL '1000 MINUTE'"""
	cursor.execute(q.format(prediction,u_city))

	result = cursor.fetchall()
	l = []
	for item in result:
		user_name = item[1]
		user_id = item[0]
		l.append([user_name, user_id, u_city])
	context = dict(data=l)
	return render_template("result2.html", **context)

@app.route("/result_more", methods = ['POST'])
def more():
	uid = request.form['uid']
	q = """SELECT t2.business_names, t3.user_name, t2.business_city, t1.review_stars from clean_review t1 \
			join clean_business t2 \
			on t1.business_ids=t2.business_ids \
			join clean_user t3 \
			on t1.user_ids=t3.user_ids \
			where t1.user_ids = '{}' \
			order by t1.review_stars desc"""
	cursor.execute(q.format(uid))

	result = cursor.fetchall()
	l = []
	for item in result:
		business_names = item[0]
		user_name = item[1]
		business_city = item[2]
		review_stars = item[3]
		l.append([business_names, user_name, business_city, review_stars])
	context = dict(data=l)
	return render_template("result_more.html", **context)



if __name__ == "__main__":
	import click
	#
	# @click.command()
	# @click.option('--debug', is_flag=True)
	# @click.option('--threaded', is_flag=True)
	# @click.argument('HOST', default='0.0.0.0')
	# @click.argument('PORT', default=8111, type=int)


	# def run(debug, threaded, host, port):
	def run():
		#HOST, PORT = host, port
		#print("running on %s:%d" % (HOST, PORT))
		# app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)
		app.run(debug=True, host = '0.0.0.0', threaded = True, port= 8111)
	run()