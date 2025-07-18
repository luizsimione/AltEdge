import twint
import pandas as pd

def run_twint_search(username, since):
    c = twint.Config()
    c.Username = username
    c.Since = since
    c.Pandas = True
    c.hide_output = True
    twint.run.Search(c)
    
    return twint.storage.panda.Tweets_df