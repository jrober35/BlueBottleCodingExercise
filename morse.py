import psycopg2
import csv
import requests
import datetime

def loadDatabase():
	conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
	cur = conn.cursor()

	cur.execute("""
	CREATE TABLE sales(
		local_created_at timestamp,
		item text,
		net_quantity integer,
		temperature integer
	)
	""")

	url = 'https://api.darksky.net/forecast/4e0f00e05cb7e94eb00423400ffe686c/37.831106,-122.254110,'

	with open('morse.csv', 'r') as f:
		reader = csv.reader(f)
		next(reader)
		ctr = 0
		for row in reader:
			line = list(row)
			date = datetime.datetime.strptime(line[0], "%m/%d/%Y %H:%M:%S").date()
			start_time = datetime.datetime.strptime(line[0], "%m/%d/%Y %H:%M:%S").time()
			req = url+str(date)+'T'+str(start_time)+'?exclude=hourly'
			print(req)
			resp = requests.get(req)
			data = resp.json()
			temp = data['currently']['temperature']
			row.append(int(temp))
			cur.execute(
				"INSERT INTO sales VALUES (%s, %s, %s, %s)", row
			)
			ctr += 1
			if ctr > 999:
				break
	conn.commit()

def main():
	loadDatabase(); #Create table, download temperature data, and load into Postgresql database

if __name__ == "__main__":
	main()