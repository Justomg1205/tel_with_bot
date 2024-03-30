import time as t
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time
from decimal import Decimal

import pandas as pd
import os
import requests
import telebot
import pandas as pd
from datetime import datetime
import os

# Initialize Telebot
bot = telebot.TeleBot("7147920196:AAGhsMweYNsPAnj5IrzxOwfdbUQaDCSaxHs")

@bot.message_handler(func=lambda msg: True)
def send_welcome(message):
    bot.reply_to(message, "Hello, how are you?")

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'Sec-Fetch-User': '?1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
}
def round_decimal_values(value):
    """
    Method for round the decimal values
    """
    if isinstance(value, Decimal):
        return round(value, 2)
    return value


def get_nearest_expiry_date(expiry_dates):
    current_date = datetime.now()
    nearest_expiry_date = min(
        expiry_dates, key=lambda x: abs(datetime.strptime(x, "%d-%b-%Y") - current_date)
    )
    return nearest_expiry_date


def filter_data_by_expiry_date(data, expiry_date):
    filtered_data = []
    for key, value in data.items():
        if key == "records":
            try:
                current_price = value["data"][0]["PE"]["underlyingValue"]
            except Exception as e:
                current_price = value["data"][0]["CE"]["underlyingValue"]

            for record in value["data"]:
                try:
                    if record["expiryDate"] == expiry_date:
                        data_ce = record["CE"]
                        data_pe = record["PE"]
                        del record["CE"]
                        del record["PE"]
                        record["CMP"] = current_price
                        record["CE_openInterest"] = data_ce["openInterest"]
                        record["CE_changein_openInterest"] = data_ce[
                            "changeinOpenInterest"
                        ]
                        record["PE_openInterest"] = data_pe["openInterest"]
                        record["PE_changein_openInterest"] = data_pe[
                            "changeinOpenInterest"
                        ]
                        filtered_data.append(record)
                except Exception as err:
                    continue

    df = pd.DataFrame(filtered_data)
    df["absolute_difference"] = abs(df["strikePrice"] - df["CMP"].iloc[0])
    del df["CMP"]
    nearest_index = df["absolute_difference"].idxmin()

    # Get the indices of the next 8 and previous 8 values
    nearest_plus_8_indices = list(
        range(nearest_index + 1, min(nearest_index + 9, len(df)))
    )
    nearest_minus_8_indices = list(range(max(nearest_index - 8, 0), nearest_index))

    # Combine indices to keep
    indices_to_keep = nearest_minus_8_indices + [nearest_index] + nearest_plus_8_indices

    # Filter the DataFrame to keep only the desired indices
    df = df.loc[indices_to_keep]
    df.loc[:, "PCR"] = (
        df["PE_changein_openInterest"].sum() / df["CE_changein_openInterest"].sum()
    )
    df.loc[:, "Diff"] = (
        df["PE_changein_openInterest"].sum() - df["CE_changein_openInterest"].sum()
    )
    df.loc[:, "Total_CE_OI"] = df["CE_changein_openInterest"].sum()
    df.loc[:, "Total_PE_OI"] = df["PE_changein_openInterest"].sum()
    df.loc[:, "Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.loc[nearest_index]

    return df[['PCR','strikePrice']]


def nse_data_to_pcr_calculation(index_type):

    try:
        if index_type in ["FINNIFTY", "NIFTY", "BANKNIFTY", "MIDCPNIFTY"]:
            value = "indices"
        else:
            value = "equities"
        option_chain_data = ""
        try:
            try:
                option_chain_data = requests.get(f"https://www.nseindia.com/api/option-chain-{value}?symbol={index_type}",headers=headers).json()
            except ValueError:
                s =requests.Session()
                option_chain_data = s.get("http://nseindia.com",headers=headers)
                option_chain_data = s.get(f"https://www.nseindia.com/api/option-chain-{value}?symbol={index_type}",headers=headers).json()
        except Exception as err:
            s =requests.Session()
            option_chain_data = s.get(f"https://www.nseindia.com/api/option-chain-{value}?symbol={index_type}",headers=headers)
            if option_chain_data.status_code == 200 :
                option_chain_data = option_chain_data.json()
            else:
                return str(option_chain_data)
            
        expiry_dates = option_chain_data["records"]["expiryDates"]
        nearest_expiry_date = get_nearest_expiry_date(expiry_dates)

        filtered_data = filter_data_by_expiry_date(
            option_chain_data, nearest_expiry_date
        )
        filtered_data = str(dict(filtered_data))
        return filtered_data             
    except Exception as e:
        print("error: ", traceback.format_exc())

while 1:
        
    @bot.message_handler(func=lambda msg: True)
    def send_welcome(message):
        bot.reply_to(message, "Hello, how are you?")    
    symbol = ["NIFTY","FINNIFTY","BANKNIFTY","MIDCPNIFTY"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = pool.map(nse_data_to_pcr_calculation,symbol)
        data = []
        for i,result in enumerate(results):
            data.append(f"{symbol[i]}:,{result},")
        data = str(data).replace(',','\n')
        bot.send_message(-1001902391140,data[1:-1])
    t.sleep(30)
    bot.infinity_polling()
    t.sleep(60*5)
