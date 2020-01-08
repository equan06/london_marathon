from bs4 import BeautifulSoup
import requests
import re
from os import mkdir
import sqlite3



def sql_connect(dbname):
    """
    Input: name of the sqlite3 database file (xxx.sqlite3)
    Ouput: sqlite3 connection object
    """
    relpath = dbname
    dirname = os.path.dirname(__file__)
    dbpath = os.path.join(dirname, relpath)
    print(dbpath)
    con = sqlite3.connect(dbpath)
    return con

def get_ath_data(ath_soup):
    """
    Input: a BeautifulSoup object of an athlete's results page and parse the HTML for split times, gender, age group, etc.
    Ouput: a dict containing all athlete attributes
    """
    ath_dict = {}

    # omit names from data, index athetes by bib/runner number
    ath_dict['id'] = ath_soup.find('th', text='Runner Number').find_next('td').string
    name = ath_soup.find('th', text='Name').find_next('td').string
    ath_dict['country'] = re.search(r'\(([A-Z]+)\)', name).group(1)
    ath_dict['sex'] = 'M'
    ath_dict['club'] = ath_soup.find('th', text='Club').find_next('td').string
    ath_dict['place_gender'] = ath_soup.find('th', text='Place (Gender)').find_next('td').string
    ath_dict['place_category'] = ath_soup.find('th', text='Place (Category)').find_next('td').string
    ath_dict['place_overall'] = ath_soup.find('th', text='Place (Overall)').find_next('td').string
    ath_dict['finish'] = ath_soup.find('th', text='Finish Time').find_next('td').string

    for dist in ['5K', '10K', '15K', '20K', 'Half', '25K', '30K', '35K', '40K', 'Finish']:
        split = ath_soup.find('th', text=dist).find_next_siblings('td')
        ath_dict[f'split_{dist}'] = split[1].string
        ath_dict[f'split_{dist}_diff'] = split[2].string

    print(ath_dict)



con = sqlite3.connect('data/test.db')
cur = con.cursor()
mne_sql = """
CREATE TABLE male_nonelite (
    id INTEGER PRIMARY KEY,
    country TEXT,
    sex TEXT,
    club TEXT,
    place_gender INTEGER,
    place_category INTEGER,
    place_overall INTEGER,
    finish TEXT,
    split_5K TEXT,
    split_5K_diff TEXT,
    split_10K TEXT,
    split_10K_diff TEXT,
    split_15K TEXT,
    split_15K_diff TEXT,
    split_20K TEXT,
    split_20K_diff TEXT,
    split_Half TEXT,
    split_Half_diff TEXT,
    split_25K TEXT,
    split_25K_diff TEXT,
    split_30K TEXT,
    split_30K_diff TEXT,
    split_35K TEXT,
    split_35K_diff TEXT,
    split_40K TEXT,
    split_40K_diff TEXT,
    split_Finish TEXT,
    split_Finish TEXT
)
"""


urlleft = 'https://results.virginmoneylondonmarathon.com/2019/?page='
urlright = '&event=MAS&num_results=1000&pid=list&search%5Bsex%5D=M&search%5Bage_class%5D=%25'

for page in range(1, 2):
    page_r = requests.get(urlleft + str(page) + urlright)
    soup = BeautifulSoup(page_r.content, "html.parser")
    ath_pages = soup.select('h4 > a')
    for ath_page in ath_pages:
        ath_url = 'https://results.virginmoneylondonmarathon.com/2019/' + ath_page.get('href')
        ath_r = requests.get(ath_url)
        ath_soup = BeautifulSoup(ath_r.content, "html.parser")
        get_ath_data(ath_soup)
    print(f'Page {page} parsed.')
