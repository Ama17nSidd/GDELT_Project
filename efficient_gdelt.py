# File to ingest 30-120 days of GDELT Data

import gdelt
import os
import numpy as np
import pandas as pd
from datetime import date, timedelta

gd2 = gdelt.gdelt(version=2)

def query_gdelt_events(start_date_str, num_days, date_format = "%Y %m %d"):
    start_date = date.fromisoformat(start_date_str) 
    final_df = pd.DataFrame()
    for day in range(num_days):
        #Getting the day's worth of data
        current_date = start_date + timedelta(days = day)
        date_string = current_date.strftime(date_format)
        data = gd2.Search(date_string, table = 'events', coverage = True)

        filtered_df = data[['GLOBALEVENTID', 'SQLDATE', 'GoldsteinScale', 'AvgTone', 'NumArticles', 'QuadClass', 'ActionGeo_CountryCode']]
        final_df = pd.concat([final_df, filtered_df], ignore_index=True)
    
    return final_df
        
def main():
    cache_file = 'gdelt_data.pkl'

    if os.path.exists(cache_file):
        print(f"Loading data from {cache_file}...")
        df = pd.read_pickle(cache_file)
    else:
        print("Querying GDELT data...")
        df = query_gdelt_events('2025-11-29', 60)
        print(f"Saving data to {cache_file}...")
        df.to_pickle(cache_file)
    
    new_df = df.groupby(['ActionGeo_CountryCode', 'SQLDATE']).agg({
        'AvgTone' : 'mean',
        'NumArticles' : 'sum',
        'QuadClass' : lambda x: (x==4).sum() / ((x==1).sum() + 1)
    })

    print(new_df.columns)

    print(new_df.iloc[30, 0])



    


if __name__ == "__main__":
    main()
