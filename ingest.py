import pandas as pd
from model import Application,ApplicationRepository,DataSource,Schema,DataMetrics

def main():

    app = Application(name=Application.fetch_file_name())

    app_repo = ApplicationRepository(location=ApplicationRepository.fetch_git_location(),application=app)

    app_tech = pd.read_csv(
        "data/AppTech.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    app_tech_ds = DataSource(location="data/AppTech",format="csv")

    app_tech_schema = Schema(fields=Schema.extract_fields_from_dataframe(df=app_tech),\
                             data_source=app_tech_ds)
    
    app_tech_metric = DataMetrics(metrics=DataMetrics.extract_metrics_from_dataframe(df=app_tech),\
                                  schema=app_tech_schema)
          
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