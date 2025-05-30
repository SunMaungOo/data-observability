import pandas as pd

def main():

    all_assets = pd.read_csv(
        "data/monthly_assets.csv",
        parse_dates=["Date"]
    )

    app_tech = all_assets[all_assets['Symbol'] == 'APCX']

    buzz_feed = all_assets[all_assets['Symbol'] == 'BZFD']
    buzz_feed['Intraday_Delta'] = buzz_feed['Adj Close'] - buzz_feed['Open']

    app_tech['Intraday_Delta'] = app_tech['Adj Close'] - app_tech['Open']

    kept_values = ['Open', 'Adj Close', 'Intraday_Delta']

    buzz_feed[kept_values].to_csv("data/report_buzzfeed.csv", index=False)

    app_tech[kept_values].to_csv("data/report_appTech.csv", index=False)

if __name__=="__main__":
    main()