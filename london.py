from bs4 import BeautifulSoup
import requests
import re
import os
import sqlite3
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime

"""
This file scrapes the London Marathon results page. It must be reconfigured for every year,
since HTML differs from year to year. 
"""


def sql_connect(dbname):
    """
    Input: dbname: name of db file (dbname.db) in local dir
    Ouput: sqlite3 connection object
    """
    dirname = os.path.dirname(__file__)
    dbpath = os.path.join(dirname, dbname)
    print('Connecting to: ' + dbpath)
    try:
        con = sqlite3.connect(dbpath)
    except:
        print('SQL connection error')
        raise
    return con

def sql_create_table(con, table_name):
    """
    Input: con: SQL cursor object, table_name: String
    Output: SQL cursor object
    """
    cur = con.cursor()
    sql = f"""
    CREATE TABLE {table_name} (
        name TEXT,
        country TEXT,
        club TEXT,
        category TEXT,
        id INTEGER PRIMARY KEY,
        place_gender INTEGER,
        place_category INTEGER,
        place_overall INTEGER,
        gender TEXT,
        elite TEXT,
        split_5K TEXT,
        split_5K_diff TEXT,
        split_10K TEXT,
        split_10K_diff TEXT,
        split_15K TEXT,
        split_15K_diff TEXT,
        split_20K TEXT,
        split_20K_diff TEXT,
        split_Half TEXT NOT NULL,
        split_Half_diff TEXT,
        split_25K TEXT,
        split_25K_diff TEXT,
        split_30K TEXT,
        split_30K_diff TEXT,
        split_35K TEXT,
        split_35K_diff TEXT,
        split_40K TEXT,
        split_40K_diff TEXT,
        split_Finish TEXT NOT NULL,
        split_Finish_diff TEXT,
        year INTEGER
    )
    """
    try:
        cur.execute(sql)
        print(f'Table: {table_name} created')
    except:
        print('Table created already')
    return cur

def sql_insert(con, table_name, ath):
    """
    Input: cur: SQL connection object, table_name: String, ath: list containing individual athlete values
    Output: SQL cursor object
    """
    cur = con.cursor()
    sql = f"""
    INSERT INTO {table_name}
    (name, country, club, category, id, place_gender, place_category, place_overall, gender, elite,
    split_5K, split_5K_diff, split_10K, split_10K_diff, split_15K, split_15K_diff,
    split_20K, split_20K_diff, split_Half, split_Half_diff, split_25K, split_25K_diff,
    split_30K, split_30K_diff, split_35K, split_35K_diff, split_40K, split_40K_diff,
    split_Finish, split_Finish_diff, year) VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cur.execute(sql, ath)
    except:
        print('SQL Insert failure at ')
        print(ath)
        # there are 2 runners with the same bib #...
        ath[4] = ath[4] + '999999'
    return cur

def get_ath_data(ath_soup):
    """
    Input: ath_soup: BeautifulSoup object of an athlete's results page
    Ouput: dict containing all 28 athlete attributes
    """
    ath = []
    # omit names from data, index athetes by bib/runner number

    vars = ['Name', 'Club', 'Category', 'Runner no', 'Place (M/W)', 'Place (AC)', 'Place (overall)']
    soup = ath_soup.find_all('th', text=vars)
    name_found = False
    flag = False
    for s in soup:
        if s.text == 'Name':
            if name_found:
                continue
            else:
                name = s.find_next('td').string
                name_found = True
                try:
                    country = re.search(r'\(([A-Z]+)\)', name).group(1)
                except:
                    flag = True
                    country = 'None'
                ath.append(name)
                ath.append(country)
        else:
            res = s.find_next('td').string
            ath.append(res)
    ath.append('M')
    ath.append('nonelite')


    vars = ['5K', '10K', '15K', '20K', 'HALF', '25K', '30K', '35K', '40K', 'Finish time']
    soup = ath_soup.find_all('th', text=vars)
    try:
        # 2013 Finish time is listed twice as an element, so skip the first occurrence
        for s in soup[1:]:
            res = s.find_next_siblings('td')
            ath.append(res[1].string)
            ath.append(res[2].string)
    except:
        print('error')
        print(ath)
    ath.append('2013')
    ath[4] = ath[4] + '99992013'
    if flag:
        print(ath)
    return ath




def page_opener(ath_page):
    ath_url = 'https://results.virginmoneylondonmarathon.com/2013/' + ath_page.get('href')
    ath_r = requests.get(ath_url)
    ath_soup = BeautifulSoup(ath_r.content, "html.parser")
    ath = get_ath_data(ath_soup)
    return ath

if __name__ == '__main__':

    con = sql_connect(f'nonelite.db')
    table_name = 'nonelite'
    # run once initially
    sql_create_table(con, table_name)

    # change M to W under sex, change MAS to ELIT under event
    # 2013 url change
    urlleft = 'https://results.virginmoneylondonmarathon.com/2013/index.php?page='
    urlright = '&event=MAS&num_results=1000&pid=search&search%5Bsex%5D=M&search_sort=place_nosex&split=time_finish_netto'


    count = 0
    try:
        for page in range(1, 24):
            curr_time = datetime.now()
            page_r = requests.get(urlleft + str(page) + urlright)
            soup = BeautifulSoup(page_r.content, "html.parser")
            # ignore every other url
            ath_pages = soup.select('td > a')[::2]
            pool = ThreadPool(4)
            res = pool.map(page_opener, ath_pages)
            pool.close()
            pool.join()
            # this is a workaround; we want only the even numbered indices

            for ath in res:
                sql_insert(con, table_name, ath)
                count+=1
            print(count)
            # commit changes every page = 1000 rows
            con.commit()
            new_time = datetime.now()
            elapsed_time = (new_time - curr_time).total_seconds() / 60
            print(f'Commit successful')
            print(f'Table: {table_name} | Page: {page} complete |  Time: {elapsed_time} min')

    except:
        print(f'Failed on Page: {page}')
        con.rollback()
        print('Rollback to last commit successful.')
        con.close()
        raise
