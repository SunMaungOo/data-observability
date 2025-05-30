import pandas as pd
import json
from model import (
    Application,
    ApplicationRepository,
    observations_for_df,
    OutputDataLineage,
    ApplicationVersion,
    User,
    ApplicationExecution,
    DataLineageExecution,
    DataMetrics
)

def main():

    app = Application(name=Application.fetch_file_name())

    print(json.dumps(app))

    app_repo = ApplicationRepository(location=ApplicationRepository.fetch_git_location(),application=app)

    print(json.dumps(app_repo))
    

    user = User(name=User.fetch_git_author())

    print(json.dumps(user))


    app_version = ApplicationVersion(version=ApplicationVersion.fetch_git_version(),\
                                     user=user,\
                                     application_repo=app_repo)
    
    print(json.dumps(app_version))

    app_exec = ApplicationExecution(application_version=app_version,\
                                    user=user)

    print(json.dumps(app_exec))

    app_tech = pd.read_csv(
        "data/AppTech.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })

    (app_tech_ds,app_tech_schema) = observations_for_df(df_name="data/AppTech.csv",\
                        df_format="csv",\
                        df=app_tech)  


    buzz_feed = pd.read_csv(
        "data/Buzzfeed.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })
    
    (buzz_feed_ds,buzz_feed_schema) = observations_for_df(df_name="data/Buzzfeed.csv",\
                        df_format="csv",\
                        df=buzz_feed)
        
    monthly_assets = pd.concat([app_tech,buzz_feed]).astype(
        {
            "Symbol":"category"
        }
    )

    monthly_assets.to_csv("data/monthly_assets.csv",index=False)

    (monthly_assets_ds,monthly_assets_schema)  = observations_for_df(df_name="data/monthly_assets.csv",\
                        df_format="csv",\
                        df=monthly_assets)  
    
        
    lineage = OutputDataLineage(schema=monthly_assets_schema,\
                      input_schemas_mapping=OutputDataLineage.generate_direct_mapping(output_schema=monthly_assets_schema,\
                                                                                      input_schemas=[app_tech_schema,buzz_feed_schema]))
    
    print(json.dumps(lineage))


    lineage_exec = DataLineageExecution(
        lineage=lineage,\
        application_execution=app_exec
    )

    print(json.dumps(lineage_exec))

    app_tech_metric = DataMetrics(
        metrics=DataMetrics.extract_metrics_from_dataframe(df=app_tech),
        schema=app_tech_schema,\
        lineage_execution=lineage_exec
    )

    print(json.dumps(app_tech_metric))


    buzz_feed_metric =  DataMetrics(
        metrics=DataMetrics.extract_metrics_from_dataframe(df=buzz_feed),
        schema=buzz_feed_schema,\
        lineage_execution=lineage_exec
    )

    print(json.dumps(buzz_feed_metric))

    monthly_assets_metric = DataMetrics(
        metrics=DataMetrics.extract_metrics_from_dataframe(df=monthly_assets),
        schema=monthly_assets_schema,\
        lineage_execution=lineage_exec
    )

    print(json.dumps(monthly_assets_metric))


if __name__=="__main__":
    main()