import psycopg2 # package useful for connecting to postgres database 
import os
import logging


logging.basicConfig(filename = 'database.log', level= logging.DEBUG, format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
logging.info('Final Database.py log')

# connect to postgresql database set up with docker
conn = psycopg2.connect(
    user = "postgres",
    password = "password",
    host = "0.0.0.0",
    port = "5432"

)
logging.info('Connected to postgres database')


# create cursor to query database
cur = conn.cursor()
logging.info('Queryed database')

logging.info('Creating schemas')

# schema for dimension tables
cur.execute("create schema dim;")

# schema for raw data 
cur.execute("create schema landing;")
# schema for fact table
cur.execute("create schema fact;")

# hold raw data w.o. data relations between dimension and fact table (straight from CSV file)
cur.execute("create table landing.rawdata( Title TEXT, Release_Year date, Genre TEXT, \
            Directors TEXT, Stars TEXT, Synopsis TEXT, Runtime float, MPAA TEXT, image_url \
            TEXT, Score float);")


#load raw data from transformed csv file to postgres database based on csv col names
cwd = os.getcwd()
print
# cur.execute("COPY landing.rawdata FROM " + cwd + "movies_by_genre_prefin.csv' DELIMITER ',' CSV HEADER;")
cur.execute("COPY landing.rawdata FROM '/Users/dannyy/Desktop/Yassky_Daniel_NWO_Submission/movies_by_genre_fin.csv' DELIMITER ',' CSV HEADER;")

# add index primary key (id) to serialize the raw data in landing.rawdata table
cur.execute("alter table landing.rawdata add id serial;")
# dimension table for movie release year
cur.execute("create table dim.Release_Year as select \
    row_number() Over(order by Release_Year) as Release_Yearid \
    ,Release_Year from  (select distinct Release_Year from landing.rawdata) t;")

# dimension table for movie genre set (foreign key rather than n:M relationship for genre category)
# dimension table describes a genre category as the top 3 genres to which a movie belongs
# i.e. a genre set may be ("Action", "Adventure", "Romance") rather than a single genre
cur.execute("create table dim.Genre as select \
    row_number() Over(order by Genre) as Genreid \
    ,Genre from  (select distinct Genre from landing.rawdata) t;")

# dimension table for movie directors
cur.execute("create table dim.Directors as select \
    row_number() Over(order by Directors) as Directorsid \
    ,Directors from  (select distinct Directors from landing.rawdata) t;")

# dimension table for movie's star cast
cur.execute("create table dim.Stars as select \
    row_number() Over(order by Stars) as Starsid \
    ,Stars from  (select distinct Stars from landing.rawdata) t;")

# dimension table for movie MPAAA rating (i.e. G, PG, ETC)
cur.execute("create table dim.MPAA as select \
    row_number() Over(order by MPAA) as MPAAid \
    ,MPAA from  (select distinct MPAA from landing.rawdata) t;")

logging.info('Finished creating DIM tables')

# movie fact table to be called fact.movie
# - Title, Runtime, , image_url, score (float) are unique to every movie so they remain in the fact table
cur.execute("create table fact.movie as select \
    q.id \
    ,r.Release_Yearid \
    ,g.Genreid \
    ,d.Directorsid \
    ,s.Starsid \
    ,m.MPAAid \
    ,q.Title \
    ,q.Synopsis \
    ,q.Runtime \
    ,q.image_url \
    ,q.score \
from \
    landing.rawdata q \
    JOIN dim.Release_Year as r on q.Release_Year = r.Release_Year \
    JOIN dim.Genre as g on q.Genre = g.Genre \
    JOIN dim.Directors as d on q.Directors = d.Directors \
    JOIN dim.Stars as s on q.Stars = s.Stars \
    JOIN dim.MPAA as m on q.MPAA = m.MPAA;")

logging.info('Created FACT table called fact.movie')


#### VALIDATION
# find first entry in the table
postgreSQL_select_Query = "select * from fact.movie where id = 1"

cur.execute(postgreSQL_select_Query)
records = cur.fetchall()
logging.info('fetched the first record in the fact table' + str(records))


postgreSQL_select_Query = "select * from fact.movie where id = 100"

cur.execute(postgreSQL_select_Query)
records = cur.fetchall()
logging.info('fetched the 100th record in the fact table' + str(records))


postgreSQL_select_Query = "select * from fact.movie where id = 1000"

cur.execute(postgreSQL_select_Query)
records = cur.fetchall()
logging.info('fetched the 1000th record in the fact table' + str(records))

postgreSQL_select_Query = "select * from fact.movie where genreid = 5"


cur.execute(postgreSQL_select_Query)
records = cur.fetchall()
logging.info('fetched the record in the fact table where genreid = 5' + str(records))

conn.commit()

# close connection once finished
conn.close()