import urllib3
import mysql.connector
from mysql.connector import errorcode
import re
from bs4 import BeautifulSoup
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

mydb = None
# The website to be scraped
ROOT_URL = "https://news.ycombinator.com/"
http = urllib3.PoolManager()
posts = []
times = []


def db_connect():
    global mydb
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      passwd="",
    )
    mycursor = mydb.cursor()
    mycursor.execute("SHOW DATABASES")
    flag = False
    for db in mycursor:
        if "scrapping" in db:
            flag = True;
    # if database does not exist, create a new database
    if flag == False:
        mycursor.execute("CREATE DATABASE scrapping")
        mycursor.execute("CREATE TABLE jobs (id INT  PRIMARY KEY, company VARCHAR(255), position VARCHAR(255), location VARCHAR(255))")

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="scrapping"
    )
    mycursor = mydb.cursor()

    flagT = False
    mycursor.execute("SHOW TABLES")
    for table in mycursor:
        if "jobs" in table:
            flagT = True;
    # if table does not exist in database, create a new table
    if flagT == False:
        mycursor.execute("CREATE TABLE jobs (id INT  PRIMARY KEY, company VARCHAR(255), position VARCHAR(255), location VARCHAR(255))")


# This method returns the most latest job that exist in table
def get_max_id():
    try:
        db_connect()
        select_sql = ('SELECT MAX(id) FROM jobs')

        # Get the max id executing SELECT with the cursor
        cursor = mydb.cursor()
        cursor.execute(select_sql)

        result = cursor.fetchone()
        max_id = result[0]

        # If the table is empty than we will set the max_id to 0
        if max_id is None:
            return 0
        else:

            # Close the cursor
           # mycursor.close()
            return max_id

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Something is wrong with your username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('Database does not exist')
        else:
            print('Some error occurred')


# This method scraps the data from one page and checks if next page exists.
def get_links(url, posts, times):
    req = http.request('GET', ROOT_URL + url)
    if req.status != 200:
        print("Error:", req.status, "skipping page", url)
        return
    page = req.data
    soup = BeautifulSoup(page, "html.parser")
#    posts += soup.find_all("a", {"class": "storylink"})
    times += soup.find_all("span",  {"class": "age"})

    table = soup.find("table", attrs={"class": "itemlist"})
    max_id = get_max_id()
    # Store the required data in an array
    for elem in table.findAll('tr', attrs={'class': 'athing'}):
        elem_id = elem.get('id')

        '''
        To only scrape the new information we can compare the id of the job
        listing with the maximum id we have in our database,
        as the id is incremental
        '''
        if int(elem_id) > max_id:
            posts.append({'id': elem_id,
                          'info': elem.find('a',
                                               attrs={'class': 'storylink'}).text})

    next_url = soup.find("a", {"class": "morelink"})["href"]

    return next_url, posts,times


next_url, posts,times = get_links("jobs", posts, times)
while next_url:
    try:
        next_url, posts, times = get_links(next_url, posts, times)
    except TypeError:
        next_url = None
'''
Here after applying the regex pattern simple string is separated 
into company, job position and it's location and processed data
is inserted into db
'''
for post in posts:
    regex = re.compile(r"\s*[Ii]s [Hh]iring|[Hh]iring|[Ii]s [Ll]ooking\s*")
    arr = re.split(regex, post['info'])

    if len(arr) > 1:
        if arr[1].find(" in") > -1:
            temp = arr[1].split(" in")
            arr.pop(1)
            arr.extend(temp)

    print(arr)
    sql = "INSERT INTO jobs (id, company, position, location) VALUES (%s, %s, %s, %s)"
    if len(arr) == 3:
        val = (int(post['id']), arr[0], arr[1], arr[2])
    elif len(arr) == 2:
        val = (int(post['id']), arr[0], arr[1], " N/A")
    else:
        val = (int(post['id']), arr[0], "N/A", " N/A")

    mycursor = mydb.cursor()
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
    mycursor.close()