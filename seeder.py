from flask import Flask
from flask_mysqldb import MySQL
from flask import jsonify
import random
import datetime
import math
app = Flask(__name__)

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'statistics'
 
mysql = MySQL(app)


@app.route("/")
def seed():
    totalDays = 10
    today = datetime.datetime.today()
    totalRecords = 200000
    dateParts = totalRecords / totalDays 
    transactionTypes = ['incoming','outgoing','refund']
    #Creating a connection cursor
    cursor = mysql.connection.cursor()
    for i in range(totalRecords):
        userId = random.randint(1,100)
        trType = random.choices(transactionTypes, weights = [80, 15, 5])[0]
        amount = random.randint(1000,1000000)
        createdAt = (today - datetime.timedelta(days=(math.floor(i/dateParts)+1 ),minutes=random.randrange(1, 300,5))).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO transactions (user_id,type,amount,created_at) VALUES({},'{}',{},'{}')".format(userId,trType,amount,createdAt))
        mysql.connection.commit()
    #Closing the cursor
    cursor.close()
    return "<p>Done!</p>"