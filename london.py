from bs4 import BeautifulSoup
import requests
import re
import os
import sqlite3



def sql_connect(dbname):
    """
    Input: name of the sqlite3 database file (xxx.sqlite3)
    Ouput: sqlite3 connection object
    """
    dirname = os.path.dirname(__file__)
    dbpath = os.path.join(dirname, dbname)
    print('Connecting to: ' + dbpath)
    try:
        con = sqlite3.connect(dbpath)
    except Error as e:
        print(e)
    return con

def sql_create_table(con, table_name):
    """
    Input: con: SQL cursor object, table_name: String
    Output: SQL cursor object
    """
    cur = con.cursor()
    sql = f"""
    CREATE TABLE {table_name} (
        id INTEGER PRIMARY KEY,
        country TEXT,
        gender TEXT,
        club TEXT,
        place_gender INTEGER,
        place_category INTEGER,
        place_overall INTEGER,
        finish TEXT NOT NULL,
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
        split_Finish_diff TEXT
    )
    """
    cur.execute(sql)
    print(f'Table: {table_name} created')
    return cur

def sql_insert(con, table_name, ath):
    """
    Input: cur: SQL connection object, table_name: String, ath: list containing individual athlete values
    Output: SQL cursor object
    """
    cur = con.cursor()
    sql = f"""
    INSERT INTO {table_name}
    (id, country, gender, club, place_gender, place_category, place_overall, finish,
    split_5K, split_5K_diff, split_10K, split_10K_diff, split_15K, split_15K_diff,
    split_20K, split_20K_diff, split_Half, split_Half_diff, split_25K, split_25K_diff,
    split_30K, split_30K_diff, split_35K, split_35K_diff, split_40K, split_40K_diff,
    split_Finish, split_Finish_diff) VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, ath)
    return cur

def get_ath_data(ath_soup):
    """
    Input: ath_soup: BeautifulSoup object of an athlete's results page
    Ouput: dict containing all 28 athlete attributes
    """
    ath = []

    # omit names from data, index athetes by bib/runner number
    ath.append(ath_soup.find('th', text='Runner Number').find_next('td').string)
    name = ath_soup.find('th', text='Name').find_next('td').string
    ath.append(re.search(r'\(([A-Z]+)\)', name).group(1))
    ath.append('M')
    ath.append(ath_soup.find('th', text='Club').find_next('td').string)
    ath.append(ath_soup.find('th', text='Place (Gender)').find_next('td').string)
    ath.append(ath_soup.find('th', text='Place (Category)').find_next('td').string)
    ath.append(ath_soup.find('th', text='Place (Overall)').find_next('td').string)
    ath.append(ath_soup.find('th', text='Finish Time').find_next('td').string)

    for dist in ['5K', '10K', '15K', '20K', 'Half', '25K', '30K', '35K', '40K', 'Finish']:
        split = ath_soup.find('th', text=dist).find_next_siblings('td')
        ath.append(split[1].string)
        ath.append(split[2].string)

    return ath




urlleft = 'https://results.virginmoneylondonmarathon.com/2019/?page='
urlright = '&event=MAS&num_results=1000&pid=list&search%5Bgender%5D=M&search%5Bage_class%5D=%25'

table_name = 'male_nonelite'
con = sql_connect(f'{table_name}.db')

# run once initially?
#sql_create_table(con, table_name)

count = 11000
try:
    for page in range(11, 27):
        page_r = requests.get(urlleft + str(page) + urlright)
        soup = BeautifulSoup(page_r.content, "html.parser")
        ath_pages = soup.select('h4 > a')
        for ath_page in ath_pages:
            ath_url = 'https://results.virginmoneylondonmarathon.com/2019/' + ath_page.get('href')
            ath_r = requests.get(ath_url)
            ath_soup = BeautifulSoup(ath_r.content, "html.parser")
            ath = get_ath_data(ath_soup)
            print(ath)
            sql_insert(con, table_name, ath)
            count += 1
            if count % 100 == 0:
                print(f'Rows added: {count}')

        # commit changes every page = 1000 rows
        con.commit()
        print(f'Commit successful')
        print(f'Table: {table_name} Page: {page} complete.')

except:
    print(f'Failed on Page: {page} Row: {count}')
    con.rollback()
    print('Rollback to last commit.')
    con.close()
    raise
