import snscrape.modules.twitter as sntwitter
import pandas as pd
import schedule

def econ_ra():
    attributes_container = []
    
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:econ_ra include:nativeretweets').get_items()):
        if i>200:
            break
    attributes_container.append([tweet.user.username, tweet.date, tweet.content, tweet.mentionedUsers])
    
    tweets_df = pd.DataFrame(attributes_container, columns=["User", "Date Created", "Tweet", "Mentioned"])
    tweets_df = tweets_df[tweets_df["Tweet"].str.strip().str.len()>0]
    tweets_df = tweets_df[tweets_df["Tweet"].str.contains("RTed, thanks!") == False]
    
    return tweets_df

 schedule.every().day.at("10:30").do(econ_ra)