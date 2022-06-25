# ============================================================================ #
# START
# ============================================================================ #

# ============================================================================ #
# 1.0 Load information

# Import libraries
from dotenv import load_dotenv
import os
import requests
import json
import sqlite3
import pandas as pd
import janitor as jr
import valdezds as vds
import plotnine as p9

vds.getwd() # Current Working Directory


# ============================================================================ #
# 1.1 - Explore Crypto API
# Link: https://www.coingecko.com/en/api/documentation

# Notes from FAQs:
# Do I need API keys to access your free API?
# "Nope, our free API is freely accessible"
# No need for API KEY

# Rate limits?
# "Our free API has a rate limit of 10-50 calls per minute, 
# if you exceed that limit you will be blocked until the next 1 minute window.
# Do revise your queries to ensure that you do not exceed our limits 
# should that happen."


# ============================================================================ #
# 1.2 - Get a list of all coins with id, name and symbol (using Crypto API)

url = "https://api.coingecko.com/api/v3/coins/list"
json_data = requests.get(url).json()

# Save json data in json format
with open('Misc/mydata.json', 'w') as f:
    json.dump(json_data, f)


# ============================================================================ #
# 1.3 Get bitcoin coin id:

#   {
#     "id": "bitcoin",
#     "symbol": "btc",
#     "name": "Bitcoin"
#   },


# ============================================================================ #
# 2.0 Get the price of bitcoin in usd and by date of the first quarter of 2022
# ============================================================================ #

# ============================================================================ #
# 2.1 Bitcoin Price

# In USD
id = "bitcoin"
currency = "usd"
usd_btc_url = f"https://api.coingecko.com/api/v3/simple/price?ids={id}&vs_currencies={currency}"
usd_btc_price = requests.get(usd_btc_url).json() # Make request
usd_btc_price["bitcoin"]["usd"] # Current price

# In mexican pesos?
id = "bitcoin"
currency = "mxn"
mxn_btc_url = f"https://api.coingecko.com/api/v3/simple/price?ids={id}&vs_currencies={currency}"
mxn_btc_price = requests.get(mxn_btc_url).json() # Make request
mxn_btc_price["bitcoin"]["mxn"] # Current price


# June 24, 2022
# 21,326.0 USD = 423,660.1834 MXN
# 25 June, 01:00 UTC
# OpenExchangeRates


# ============================================================================ #
# 2.2 By date of the first quarter of 2022

# Function to retrieve historical data from one specific coin
def get_historical_data(start_date, end_date, coin_name, currency):
    
    # Interval date creation
    range_dates = (
        pd.DataFrame(
            {"start_date": pd.to_datetime([start_date]),
             "today": pd.to_datetime([pd.Timestamp("today").strftime("%Y/%m/%d")])}
        )
    )
    
    # Day difference calculation
    diff_dates = (range_dates["today"] - range_dates["start_date"]).dt.days[0]
    
    # Make request
    day_diff_url = day_diff_url = f"https://api.coingecko.com/api/v3/coins/{coin_name}/market_chart?vs_currency={currency}&days={diff_dates}&interval=daily"
    r = requests.get(day_diff_url).json()
    
    # Creating data frame from request data & range dates
    price_list = r["prices"]
    date_list = pd.date_range(start=start_date, end=range_dates["today"][0])
    data = pd.DataFrame(price_list, index = date_list, columns = ["id", "closing_price"])
    data.reset_index(inplace=True) # Bring date column to dataFrame
    
    # Filtering dataFrame with start & end dates
    begin = start_date
    end = end_date
    
    data = (
        data.rename_column(old_column_name="index", new_column_name="date")
        .query("date.between(@begin, @end)")
    )
    
    return data


# Retrieve data
df = get_historical_data(start_date="2022-01-01", end_date="2022-03-31", coin_name="bitcoin", currency="usd")

# Quick Exploratory Data Analysis
jr.get_dupes(df)
df["id"].count()
df["id"].nunique()
df.isnull().sum()
df.shape
df.info()

# Validation:
# https://www.coingecko.com/en/coins/bitcoin/historical_data#panel


# ============================================================================ #
# 2.3 Save the information in the database of your choice

# Take environment variables from .env
load_dotenv()

# Establish connection
database_path = os.getenv("dbeaver_path")
conn = sqlite3.connect(database_path)

# Create Table and Read SQL
df.to_sql("first_qrt_btc_hist_2022", conn, if_exists="replace")
pd.read_sql("SELECT * FROM first_qrt_btc_hist_2022;", conn)

conn.close() # Remember to close connection


# ============================================================================ #
# 3.0 Consume the data previously persisted in the database to make a
#     window/partition function for every 5 days
# ============================================================================ #

# Moving average calculation
avg_data = df.assign(moving_average=df["closing_price"].rolling(5).mean())

# NOTE:
# If you want to copy paste info to see it in an excel file, do the following:
avg_data.to_clipboard()


# ============================================================================ #
# 3.1 Save the information (Moving Average) in the database of your choice

# Establish connection
database_path = os.getenv("dbeaver_path")
conn = sqlite3.connect(database_path)

# Create Table and Read SQL (Moving Average)
avg_data.to_sql("first_qrt_btc_mov_avg_2022", conn, if_exists="replace")
pd.read_sql("SELECT * FROM first_qrt_btc_mov_avg_2022;", conn)

conn.close() # Remember to close connection


# ============================================================================ #
# 4.0 Show results obtained in a graph
# ============================================================================ #

# In order to identify color lines, follow tidy principles.
# Plotnine wants you to specify aes() parameters as columns
# in your dataset.
plot_tbl = avg_data.pivot_longer(
    index=["date", "id"],
    column_names=["closing_price", "moving_average"],
    names_to="type",
    values_to="value"
)

# Plot
plot_file = (
    p9.ggplot(
        data=plot_tbl,
        mapping=p9.aes(x="date", y="value")
    ) +

    # geoms
    p9.geom_line(p9.aes(color="type")) +
    p9.scale_y_continuous(
        breaks=[35000, 37000, 39000, 41000, 43000, 45000, 47000],
        labels=["35,000", "37,000", "39,000",
                "41,000", "43,000", "45,000", "47,000"]
    ) +

    # labs
    p9.labs(
        x="Date",
        y="Closing Price",
        title="Daily Bitcoin Price in the First Quarter of 2022"
    ) +

    # theme
    p9.theme(
        figure_size=(10, 6),
        panel_background=p9.element_rect(fill="snow"),
        axis_title=p9.element_text(face="bold"),
        axis_text=p9.element_text(face="italic"),
        plot_title=p9.element_text(face="bold", size=15),
        legend_text=p9.element_text(face="italic"),
        legend_title=p9.element_text(face="bold")
    )
)

# Save image
plot_file.save(filename="plot.png")



# ============================================================================ #
# END
# ============================================================================ #
