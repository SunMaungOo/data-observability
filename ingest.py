import pandas as pd
import json
from model import Application,ApplicationRepository,observations_for_df,OutputDataLineage

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

    (app_tech_ds,app_tech_schema,app_tech_metric) = observations_for_df(df_name="data/AppTech.csv",\
                        df_format="csv",\
                        df=app_tech)  

    buzz_feed = pd.read_csv(
        "data/Buzzfeed.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    (buzz_feed_ds,buzz_feed_schema,buzz_feed_metric) = observations_for_df(df_name="data/Buzzfeed.csv",\
                        df_format="csv",\
                        df=buzz_feed)
        
    monthly_assets = pd.concat([app_tech,buzz_feed]).astype(
        {
            "Symbol":"category"
        }
    )

    monthly_assets.to_csv("data/monthly_assets.csv",index=False)

    (monthly_assets_ds,monthly_assets_schema,monthly_assets_metric)  = observations_for_df(df_name="data/monthly_assets.csv",\
                        df_format="csv",\
                        df=monthly_assets,\
                        is_print_observation=False)  
        
    lineage = OutputDataLineage(schema=monthly_assets_schema,\
                      input_schemas_mapping=OutputDataLineage.generate_direct_mapping(output_schema=monthly_assets_schema,\
                                                                                      input_schemas=[app_tech_schema,buzz_feed_schema]))
    
    print(json.dumps(lineage))


if __name__=="__main__":
    main()