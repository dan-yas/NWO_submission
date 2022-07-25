import requests # Useful for getting http responses for URLs
from bs4 import BeautifulSoup # Useful for parsing html 
import pandas as pd
import re  # data cleanup
import logging 
from zipfile import ZipFile # file compression

logging.basicConfig(filename = 'extract.log', level= logging.DEBUG)

logging.info('data_extract.py final log')

## hold metadata for each grabbed imbd mov
mov_titles = []
mov_runtime = []
mov_release_year = []
imbd_score = []
mov_genres = []
mov_img = []
mov_director = []
mov_stars = []
mov_synopsis =[]
mov_mpaa = []

# urls to hit for every genre 
urls = []
def get_genre_urls(imdb_url):
    ''' get all urls for top english movies movies by genre given link to imbd homepage '''
    response = requests.get(imdb_url)
    soup = BeautifulSoup(response.text, 'html.parser') # soup of the imbd homepage
    main_soup = soup.find_all("div", {"class": "ab_links"})[7]  # div of english movies by genre
    genres  = main_soup.findChildren("div", {"class": "table-cell primary"}) 
    for genre in genres:
        url = genre.findAll('a')[0] # get all genre links
        url = url.attrs['href'] 
        url = "https://www.imdb.com" + url
        urls.append(url)
    return urls 

# array to hold url for every genre
urls = get_genre_urls("https://www.imdb.com/feature/genre?ref_=fn_asr_ge")

# use regular expression to remove clutter around a date
def clean_date(value):
    return re.sub('\(|\)', '', value)

# return the movie title from the top 
def movTitle(top):
    try:
        return top[0].find("a").getText()
    except:
        return ''

# return movie release year from html top if found
def movRelYear(top):
    try:
        return clean_date(top[0].find("span",  {"class": "lister-item-year text-muted unbold"}).getText())
    except:
        return ''
# return movie imdb rating from metadata
def movRating(meta):
    try:
        return meta.find("span", {"class": "runtime"}).getText()
    except:
        return ''

def movGenre(meta):
    try:
        return meta.find("span",  {"class":  "genre"}).getText()
    except:
        return ''

def movRuntime(meta):
    try:
        return meta.find("span", {"class": "runtime"}).getText()
    except: 
        return ''

def movMPAA(meta):
    try:
        return meta.find("span", {"class": "certificate"}).getText()
    except: 
        return ''

def movDirectors(movie):
    try:
        return movie.find("p", {"class": ""}).getText().strip().split()[1] + " " + movie.find("p", {"class": ""}).getText().strip().split()[2]
    except: 
        return ''

def movStars(movie):
    try:
        return movie.find("p", {"class": ""}).getText().strip().split("Stars:",1)[1]
    except: 
        return ''

def movSynopsys(movie):
    try:
        return movie.find_all("p", {"class":  "text-muted"})[1].getText()
    except:
        return ''

def movScore(movie):
    try:
        return float(movie.find("div", {"class": 'inline-block'}).text[2:5])
    except:
        return ''

def movImage(image):
    try:
        return image.get('loadlate')
    except:
        return ''

# gets number from text
def find_number(text):
    num = re.findall(r'[0-9]+',text)
    return float(" ".join(num))

def main(imdb_url):
    # target url (given for a specific genre) which loads the top 50 movies page for that genre
    response = requests.get(imdb_url) 
    soup = BeautifulSoup(response.text, 'lxml')

    # Movie Name
    movies_list  = soup.find_all("div", {"class": "lister-item mode-advanced"})
    
    # find meta data tags for every array type for every movie
    for movie in movies_list:
        top = movie.find_all("h3", {"class":  "lister-item-header"})
        meta = movie.find_all("p", {"class":  "text-muted"})[0]
        imageDiv =  movie.find("div", {"class": "lister-item-image float-left"})
        image = imageDiv.find("img", "loadlate")

        #  Append metadata tp respective array for that type of metadata

        # Mov title
        mov_titles.append(movTitle(top))
        
        #  Mov release year
        mov_release_year.append(movRelYear(top))
        
        # Mov Genre
        mov_genres.append(movGenre(meta))

        # Mov Runtime
        mov_runtime.append(movRuntime(meta))
        
        # Movie Synopsys
        mov_synopsis.append(movSynopsys(movie))
        
        #  Image attributes
        mov_img.append(movImage(image))

        # Director
        mov_director.append(movDirectors(movie))

        # Top Actors (stars)
        mov_stars.append(movStars(movie))

        # MPAA
        mov_mpaa.append(str(movMPAA(meta)))
               
        #Score 
        imbd_score.append(movScore(movie))


for url in urls:
    main(url)


# Attach all the data to the pandas dataframe. You can optionally write it to a CSV file as well
df = pd.DataFrame({
    "Title": mov_titles,
    "Release_Year": mov_release_year,
    "Genre": mov_genres,
    "Directors": mov_director,
    "Stars": mov_stars,
    "Synopsis": mov_synopsis,
    "Runtime": mov_runtime,
    "MPAA":  mov_runtime,
    "image_url": mov_img,
    "Score": imbd_score

})

# Validation 
logging.info(df.head())

logging.info(df.dtypes)

#more cleanup for movie release year and runtime
df['Release_Year']=df['Release_Year'].apply(lambda x: find_number(x))
df['Runtime']=df['Runtime'].apply(lambda x: find_number(x))


# converts Release_Year to date variable
df['Release_Year'] = pd.to_datetime(df['Release_Year'], format='%Y')


# Validation 
logging.info(df.head())
# see types of every column 
logging.info(df.dtypes)

file = df.to_csv("movies_by_genre_fin.csv", index = False)

zipCSV = ZipFile("movies_by_genre_fin.zip", 'w')
zipCSV.write("movies_by_genre_fin.csv")
zipCSV.close()
