import pandas as pd
import json
from model import Application,ApplicationRepository,observations_for_df

def main():

    app = Application(name=Application.fetch_file_name())

    print(json.dumps(app))

    app_repo = ApplicationRepository(location=ApplicationRepository.fetch_git_location(),application=app)

    print(json.dumps(app_repo))

    app_tech = pd.read_csv(
        "data/AppTech.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })

    observations_for_df(df_name="data/AppTech.csv",\
                        df_format="csv",\
                        df=app_tech)  

    buzz_feed = pd.read_csv(
        "data/Buzzfeed.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    observations_for_df(df_name="data/Buzzfeed.csv",\
                        df_format="csv",\
                        df=buzz_feed)  
        
    monthly_assets = pd.concat([app_tech,buzz_feed]).astype(
        {
            "Symbol":"category"
        }
    )

    monthly_assets.to_csv("data/monthly_assets.csv",index=False)

    observations_for_df(df_name="data/monthly_assets.csv",\
                        df_format="csv",\
                        df=monthly_assets)  

if __name__=="__main__":
    main()