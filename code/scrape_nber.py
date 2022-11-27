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

def nber_list():
    """Takes the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

    response = requests.get("https://www.nber.org/career-resources/research-assistant-positions-not-nber")
    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.find_all("div", {"class": "page-header__intro-inner"})[0]
    posts = text.find_all("p")
       
    clean_posts = posts[2:-2]

    output = []

    for i, v in enumerate(clean_posts): 
        if v.contents[0] != '\xa0' and not str(v.contents[0]).startswith('<em><!--'):
            title = v.contents[0] if v.contents[0] else "titlemissing"
            researcher = v.contents[2] if v.contents[2] else "researchermissing"
            institution = v.contents[5] if v.contents[5] else "instmissing"
            research_field = v.contents[7] if v.contents[7] else "fieldmissing"
            link = v.a["href"] if v.a["href"] else "linkmissing"
            output.append([title, researcher, institution, research_field, link])
    #print(output)

    return output

def posts():
    """Cleans the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

    df = pd.DataFrame(output, columns =['Title', 'Researcher', 'Institution', 'Research Field', 'Link'])

    def researcher_util(x):
        return x.split(':')[1].strip() if x.find(':') > 0 else ' '.join(x.split()[-2:])

    def field_util(x):
        return x.split(':')[1].strip() if x.find(':') > 0 else ' '.join(x.split()[-2:])


    df['Title'] = df['Title'].str.replace(r'<[^<>]*>', '', regex=True)
    df['Researcher'] = df['Researcher'].astype(str).apply(lambda row: researcher_util(row))
    df['Institution'] = df['Institution'].astype(str).apply(lambda s: re.sub('<em>|</em>|\xa0|<i>|</i>', '', s).strip() if s and s != 'instmissing' else None)
    df['Research Field'] = df['Research Field'].astype(str).apply(lambda row: field_util(row))
    df['Link'] = df['Link'].str.replace(r'<[^<>]*>', '', regex=True)


    df.set_index('Title')
    
    return df

def upload_gsheets():
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
    d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)


if __name__ == '__main__':
    print('Starting to be cool!')
    res = nber_list()
    print('len is ', len(res))
    res_posts = posts(res)
    print('len res_tweets is ', len(res_tweets)) 
    upload_gsheets(res_posts)
    print('Update completed...')
    print('I am done!')

    schedule.every().day.at("8:00").do(nber_list)
    schedule.every().day.at("8:10").do(upload_gsheets)

# # Loop so that the scheduling task
# # keeps on running all time.
    while True:
 
#     # Checks whether a scheduled task
#     # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


schedule.every().day.at("10:25").do(nber_list)
schedule.every().day.at("10:28").do(upload_gsheets)
