import gdelt
import numpy as np
import pandas as pd
from datetime import date, timedelta
import os
import seaborn as sns
import matplotlib.pyplot as plt

gd2 = gdelt.gdelt(version=2)

#helper functions

# Helper function to generate list of string for data ingestion
def generate_gdelt_query(start_date_str, num_days, date_format = "%Y %m %d"):
    
    start_date = date.fromisoformat(start_date_str)

    final_dates = []

    for day in range(num_days):
        current_date = start_date + timedelta(days = day)
        date_string = current_date.strftime(date_format)
        final_dates.append(date_string)
    return final_dates


def main():
    cache_file = 'gdelt_data.pkl'

    if os.path.exists(cache_file):
        print(f"Loading data from {cache_file}...")
        df = pd.read_pickle(cache_file)
    else:
        print("Querying GDELT data...")
        df = gd2.Search(generate_gdelt_query('2025-11-29', 30), table = 'events', coverage = True)
        print(f"Saving data to {cache_file}...")
        df.to_pickle(cache_file)

    #view all cols:
    #print(df.columns)
    
    df['date'] = pd.to_datetime(df['SQLDATE'], format='%Y%m%d')

    top_avg_gold_country = df.groupby('ActionGeo_CountryCode')['GoldsteinScale'].mean().sort_values(ascending=False).head(10)
    bot_avg_gold_country = df.groupby('ActionGeo_CountryCode')['GoldsteinScale'].mean().sort_values(ascending=True).head(10)

    #Plotting Goldstein Scale Averages, best and worst
    sns.set_theme(style = 'whitegrid')

    plt.figure(figsize=(10,6))
    sns.barplot(x = top_avg_gold_country.index, y = top_avg_gold_country.values, palette = 'viridis')
    plt.title('Top 10 Countries\' Best Average Golstein Scale')
    plt.xlabel('Country Code')
    plt.ylabel('Average Goldstein Scale')
    plt.show()

    plt.figure(figsize=(10,6))
    sns.barplot(x = bot_avg_gold_country.index, y = bot_avg_gold_country.values, palette = 'viridis')
    plt.title('Top 10 Countries\' Worst Average Golstein Scale')
    plt.xlabel('Country Code')
    plt.ylabel('Average Goldstein Scale')
    plt.show()


    print(df['EventCode'].dtype)

if __name__ == "__main__":
    main()
    




