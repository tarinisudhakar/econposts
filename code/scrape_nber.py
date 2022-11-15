import requests
from bs4 import BeautifulSoup 
import re
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import os 

"""Takes the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

response = requests.get("https://www.nber.org/career-resources/research-assistant-positions-not-nber")
html = response.text
soup = BeautifulSoup(html, "html.parser")
text = soup.find_all("div", {"class": "page-header__intro-inner"})[0]
posts = text.find_all("p")
   
clean_posts = posts[2:-2]

output = []

for i, v in enumerate(clean_posts): 
    print("index:{}".format(i))
    if v.contents[0] != '\xa0' and not str(v.contents[0]).startswith('<em><!--'):
        title = v.contents[0] if v.contents[0] else "titlemissing"
        researcher = v.contents[2] if v.contents[2] else "researchermissing"
        institution = v.contents[5] if v.contents[5] else "instmissing"
        research_field = v.contents[7] if v.contents[7] else "fieldmissing"
        link = v.a["href"] if v.a["href"] else "linkmissing"
        output.append([title, researcher, institution, research_field, link])

df = pd.DataFrame(output, columns =['Title', 'Researcher', 'Institution', 'Research Field', 'Link'])

df['Title'] = df['Title'].str.replace(r'<[^<>]*>', '', regex=True)
df['Researcher'] = df['Researcher'].str.replace(r'<[^<>]*>', '', regex=True)
df['Institution'] = df['Institution'].str.replace(r'[][]+', '', regex=True)
df['Research Field'] = df['Research Field'].str.replace(r'<[^<>]*>', '', regex=True)
df['Link'] = df['Link'].str.replace(r'<[^<>]*>', '', regex=True)

#print(df['Institution'])

#"""Uploading to Google Sheets""" 
# INPUT_DIR = "code"
# INPUT_PATH = os.path.join(INPUT_DIR, "econpostscred.json")

# scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']
# credentials = ServiceAccountCredentials.from_json_keyfile_name(
#     INPUT_PATH, scope)
# gc = gspread.authorize(credentials)


# spreadsheet_key = '1X-R8QVhi2ngDTquGgpTwH_Vquj7DCFwROLhX2rQtEl4'
# wks_name = 'nber'
# d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
