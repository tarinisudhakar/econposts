import snscrape.modules.twitter as sntwitter
import pandas as pd
import schedule
import time
import os
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import pdb 


def econ_ra():
    data = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper('@econ_ra').get_items()):
        if i>200:
            break
        data.append([tweet.date, tweet.user.username, tweet.content, tweet.url, tweet.id])

    print(data)
    return data

def tweets(data): 
    tweets_df = pd.DataFrame(data, columns=["Date Created", "User", "Tweet", "Links", "Tweet ID"])
    tweets_df = tweets_df[tweets_df["Tweet"].str.strip().str.len()>0]
    tweets_df = tweets_df[tweets_df["Tweet"].str.contains("RTed") == False]
    tweets_df = tweets_df.replace(r'\n',  ' ', regex=True)
    
    print(tweets_df)
    return tweets_df

def upload_gsheets(tweets_df):

    INPUT_DIR = "code"
    INPUT_PATH = os.path.join(INPUT_DIR, "econpostscred.json")

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        INPUT_PATH, scope)
    gc = gspread.authorize(credentials)


    spreadsheet_key = '1X-R8QVhi2ngDTquGgpTwH_Vquj7DCFwROLhX2rQtEl4'
    wks_name = 'twitter'
    #d2g.upload(tweets_df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
    wks_name.update([tweets_df.columns.values.tolist()] + tweets_df.values.tolist())

if __name__ == '__main__':
    print('Starting to be cool!')
    res = econ_ra()
    print('len is ', len(res))
    res_tweets = tweets(res)
    print('len res_tweets is ', len(res_tweets)) 
    upload_gsheets(res_tweets)
    print('Update completed...')
    print('I am done!')

    schedule.every().day.at("8:00").do(econ_ra)
    schedule.every().day.at("8:05").do(tweets)
    schedule.every().day.at("8:10").do(upload_gsheets)

# # Loop so that the scheduling task
# # keeps on running all time.
    while True:
 
#     # Checks whether a scheduled task
#     # is pending to run or not
        schedule.run_pending()
        time.sleep(1)
