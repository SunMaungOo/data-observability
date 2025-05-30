import pandas as pd

def main():

    app_tech = pd.read_csv(
        "data/AppTech.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    buzz_feed = pd.read_csv(
        "data/Buzzfeed.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    monthly_assets = pd.concat([app_tech,buzz_feed]).astype(
        {
            "Symbol":"category"
        }
    )

    monthly_assets.to_csv("data/monthly_assets.csv",index=False)
 
if __name__=="__main__":
    main()