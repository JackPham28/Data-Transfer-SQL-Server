import pyodbc
from vnstock import *
from datetime import datetime, timedelta

def connect():
	# Define connection string:
	connection_string = 'Server=TAIS-DELL\SQLEXPRESS;Database=FINPROS_DATA;Trusted_Connection=True;PORT=8391;DRIVER={SQL SERVER}'
	# Establish the connection
	conn = pyodbc.connect(connection_string)
	#Creat cursor to control retrieve database:
	cursor = conn.cursor()
	return cursor, conn
def disconnect(cursor,conn):
	cursor.close()
	conn.close()
	return None
def get_lastest_time(cursor,table):
	command = f'Select top 1 * from {table} order by time desc'
	cursor.execute(command)
	rows = cursor.fetchall()
	return rows[0][0]
def get_data(code):
	#get data from vnstock in 03 days: today, today-1, today-2.
	df =  stock_historical_data(symbol=code, start_date=str(datetime.today()-timedelta(days=2))[:10], 
		end_date=str(datetime.today())[:10], resolution='1', type='stock')
	#convert datatype of 'time' column to string because when pull data to sql server,value of cell will be auto-changed if column's type is datime64[ns, Asia/Ho Chi Minh]:
	df.time = df.time.astype(str)
	return df
def pull_data_to_sql(cursor,conn,df,table):
	pull_cmd = f'insert into {table} values (?,?,?,?,?,?,?)'
	for index, row in df.iterrows():
		cursor.execute(pull_cmd,tuple(row))
	conn.commit()
def extract_data(time,df):
	#Get the data from the "time" to newest
	return df[df.time >time]
while True:
	""" First, we create the connection to SQL server
		Second, get the latest row that we have in sql server table
		Third, get data from vnstock to 'df' for today, today-1, today-2
		Fourth, take only data from the latest time (second step) in 'df'
		Fifth, pull the data to SQL server
		Sixth, disconect and break """
	#1 Connect to SQL server:
	cur,conn =connect()
	#2,3,4,5 steps:
	table_list=['HPG','VIC','TCB','SSI','VHM']
	latest_time = None
	df = None
	df_extract = None
	for table in table_list:
		##Second:
		latest_time = get_lastest_time(cur,table)
		##Third:
		df = get_data(table)
		##Fourth:
		df_extracted = extract_data(latest_time,df)
		##Fifth:
		pull_data_to_sql(cur,conn,df_extracted,table)
	#6 disconnect
	disconnect(cur,conn)
	break