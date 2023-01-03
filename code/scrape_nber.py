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

class scheduleprint():  

    def nber_list(self):
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
        
        
        df = pd.DataFrame(dic, columns =['JobTitle', 'Researcher', 'Institution', 'FieldofResearch', 'JobLink'])
        
        def split_util(x):
            return x.split(':')[1].strip() if x.find(':') > 0 else ' '.join(x.split()[-2:])

        df['Researcher'] = df['Researcher'].astype(str).apply(lambda row: split_util(row))
        df['Institution'] = df['Institution'].astype(str).apply(lambda row: split_util(row))
        df['Institution'] = df['Institution'].str.split('[L]ink').str[0]
        df['FieldofResearch'] = df['FieldofResearch'].astype(str).apply(lambda row: split_util(row))
        df['FieldofResearch'] = df['FieldofResearch'].str.split('[L]ink').str[0]

        print(df)   

        #Uploading to Google Sheets
        INPUT_DIR = "code"
        INPUT_PATH = os.path.join(INPUT_DIR, "econpostscred.json")

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            INPUT_PATH, scope)
        client = gspread.authorize(credentials)


        spreadsheet_key = '1X-R8QVhi2ngDTquGgpTwH_Vquj7DCFwROLhX2rQtEl4'
        wks_name = 'nber'
        #d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)

        sheet = client.open('Economics RA listings_NBER and econ_ra')
        sheet_instance = sheet.get_worksheet(1)
        sheet_instance.update([df.columns.values.tolist()] + df.values.tolist())
        #sheet_instance.insert_rows(df.values.tolist()) 

    def schedule_a_print_job(self): 
        schedule.every().day.at("11:50").do(self.nber_list)

        #Loop so that the scheduling task
        #keeps on running all time.
        while True:
     
        #Checks whether a scheduled task
        #is pending to run or not
            schedule.run_pending()
            time.sleep(1)

run = scheduleprint()
run.schedule_a_print_job()
