import urllib3
import mysql.connector
import re
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup

ROOT_URL = "https://news.ycombinator.com/"
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

if flag == False:
    mycursor.execute("CREATE DATABASE scrapping")
    mycursor.execute("CREATE TABLE jobs (id INT AUTO_INCREMENT PRIMARY KEY, company VARCHAR(255), position VARCHAR(255), location VARCHAR(255))")

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

if flagT == False:
    mycursor.execute("CREATE TABLE jobs (id INT AUTO_INCREMENT PRIMARY KEY, company VARCHAR(255), position VARCHAR(255), location VARCHAR(255))")

http = urllib3.PoolManager()
posts = []
times = []
def get_links(url, posts,times):
    req = http.request('GET', ROOT_URL + url)
    if req.status != 200:
        print("Error:", req.status, "skipping page", url)
        return
    page = req.data
    soup = BeautifulSoup(page, "html.parser")
    posts += soup.find_all("a", {"class": "storylink"})
    times += soup.find_all("span",  {"class": "age"})
    next_url = soup.find("a", {"class": "morelink"})["href"]

    return next_url, posts,times


next_url, posts,times = get_links("jobs", posts, times)
while next_url:
    try:
        next_url, posts, times = get_links(next_url, posts, times)
    except TypeError:
        next_url = None

for post in posts:
    regex = re.compile(r"\s*[Ii]s [Hh]iring|[Hh]iring|[Ii]s [Ll]ooking\s*")
    arr = re.split(regex, post.text)

    if len(arr) > 1:
        if arr[1].find(" in") > -1:
            temp = arr[1].split(" in")
            arr.pop(1)
            arr.extend(temp)

    print(arr)
    sql = "INSERT INTO jobs (company, position, location) VALUES (%s, %s, %s)"
    if len(arr) == 3:
        val = (arr[0], arr[1], arr[2])
    elif len(arr) == 2:
        val = (arr[0], arr[1], " N/A")
    else:
        val = (arr[0], "N/A", " N/A")

    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
    print(times[0].text)
#print(len(times))
#print(len(posts))