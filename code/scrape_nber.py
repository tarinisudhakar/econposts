import requests
from bs4 import BeautifulSoup 
import re
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import os
import schedule 
import time
from collections import defaultdict

def nber_list():
    """Takes the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

    response = requests.get("https://www.nber.org/career-resources/research-assistant-positions-not-nber")
    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.find_all("div", {"class": "page-header__intro-inner"})[0]
    posts = text.find_all("p")
       
    clean_posts = posts[2:-2]

    joke = []
    dic = defaultdict(list)
    for i, v in enumerate(clean_posts): 
        if v.contents[0] != '\xa0' and not str(v.contents[0]).startswith('<em><!--'):
            pure = v.text.split('\n')
            link = v.a["href"] if v.a["href"] else "linkmissing"
            dic['JobTitle'].append(pure[0])
            dic['Researcher'].append([item for item in pure if 'Sponsoring' in item][0] if [item for item in pure if 'Sponsoring' in item] else 'SponsoringMissing')
            dic['Institution'].append([item for item in pure if 'Institution' in item][0] if [item for item in pure if 'Institution' in item] else 'InstitutionMissing')
            dic['FieldofResearch'].append([item for item in pure if 'Field' in item][0] if [item for item in pure if 'Field' in item] else 'FieldMissing')
            dic['JobLink'].append(link)
            pure.append(link)
            joke.append(pure)
    
    return dic 

def posts(dic):
    """Cleans the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

    df = pd.DataFrame(dic, columns =['JobTitle', 'Researcher', 'Institution', 'FieldofResearch', 'JobLink'])
    
    def split_util(x):
        return x.split(':')[1].strip() if x.find(':') > 0 else ' '.join(x.split()[-2:])

    df['Researcher'] = df['Researcher'].astype(str).apply(lambda row: split_util(row))
    df['Institution'] = df['Institution'].astype(str).apply(lambda row: split_util(row))
    df['FieldofResearch'] = df['FieldofResearch'].astype(str).apply(lambda row: split_util(row))   
    
    return df


def upload_gsheets(df):
    """Uploading to Google Sheets""" 
    INPUT_DIR = "code"
    INPUT_PATH = os.path.join(INPUT_DIR, "econpostscred.json")

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        INPUT_PATH, scope)
    gc = gspread.authorize(credentials)


    spreadsheet_key = '1X-R8QVhi2ngDTquGgpTwH_Vquj7DCFwROLhX2rQtEl4'
    wks_name = 'nber'
    #d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)

    sheet = client.open('https://docs.google.com/spreadsheets/d/1X-R8QVhi2ngDTquGgpTwH_Vquj7DCFwROLhX2rQtEl4/edit#gid=1146760227')
    sheet_instance = sheet.get_worksheet(0)
    sheet_instance.insert_rows(df.values.tolist()) 


if __name__ == '__main__':
    print('Starting to be cool!')
    result = nber_list()
    print('Length of data is ', len(result))
    posts = posts(result)
    print('Length of posts is ', len(posts)) 
    upload_gsheets(posts)
    print('Update completed...')
    print('I am done!')

# #     schedule.every().day.at("08:48").do(nber_list, result)
# #     schedule.every().day.at("08:50").do(posts, posts)
# #     schedule.every().day.at("08:52").do(upload_gsheets, posts)

# # # # Loop so that the scheduling task
# # # # keeps on running all time.
# #     while True:
 
# # #     # Checks whether a scheduled task
# # #     # is pending to run or not
# #         schedule.run_pending()
# #         time.sleep(1)