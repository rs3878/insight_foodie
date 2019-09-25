from flask import Flask, request, render_template, redirect
import numpy as np
import pandas as pd
from random import randrange
import datetime
from kafka import KafkaProducer
import pandas.io.sql as sqlio
import psycopg2


conn = psycopg2.connect("dbname=yelp user=postgres password=postgres")
cursor = conn.cursor()
engine = create_engine('postgresql://postgres:postgres@localhost:5432/yelp')


business = sqlio.read_sql_query("""SELECT * FROM clean_business""", conn)
user = sqlio.read_sql_query("""SELECT * FROM clean_user""", conn)

consumer = pd.read_sql('SELECT * FROM consumer', engine)

app = Flask(__name__)

@app.route("/", methods = ['GET','POST'])
def fcn(u_id,b_id):
	if request.method == 'POST':
		timestp = datetime.datetime(2019, 1, 1, 13, 05)
		q = """INSERT INTO yelp.public.consumer (business,user_id,time) VALUES ({},{},{})"""
		cursor.execute(q.format(u_id,b_id,timestp))
		conn.commit()
	if request.method == 'GET':
		# get user type and user city
		q = """SELECT prediction from kmeans_user where user_ids = {}"""
		r = cursor.execute(q.format(u_id))
		prediction = cursor.fetchone()
		print(prediction)
		q = """SELECT user_city from user_location where user_ids = {}"""
		r = cursor.execute(q.format(u_id))
		u_city = cursor.fetchone()
		q = """select user_name, user_ids \
				from kmeans_user \
				where prediction = {} and user_city='{}'"""
		r = cursor.execute(q.format(prediction,u_city))
		r = cursor.fetchall()
	return render_template("temp.html")

if __name__ == "__main__":
	import click

	@click.command()
	@click.option('--debug', is_flag=True)
	@click.option('--threaded', is_flag=True)
	@click.argument('HOST', default='0.0.0.0')
	@click.argument('PORT', default=8111, type=int)

	def run(debug, threaded, host, port):
		HOST, PORT = host, port
		print("running on %s:%d" % (HOST, PORT))
		app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)

	run()