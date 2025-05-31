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

    app = Application(name = Application.fetch_file_name())

    print(json.dumps(app))

    app_repo = ApplicationRepository(location=ApplicationRepository.fetch_git_location(),\
                                     application=app)

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



    all_assets = pd.read_csv(
        "data/monthly_assets.csv",
        parse_dates=["Date"]
    )

    (all_assets_ds,all_assets_schema) = observations_for_df(df_name="data/monthly_assets.csv",\
                                        df_format="csv",\
                                        df=all_assets)

    app_tech = all_assets[all_assets['Symbol'] == 'APCX']

    buzz_feed = all_assets[all_assets['Symbol'] == 'BZFD']
    buzz_feed['Intraday_Delta'] = buzz_feed['Adj Close'] - buzz_feed['Open']

    app_tech['Intraday_Delta'] = app_tech['Adj Close'] - app_tech['Open']

    kept_values = ['Open', 'Adj Close', 'Intraday_Delta']

    buzz_feed[kept_values].to_csv("data/report_buzzfeed.csv", index=False)

    app_tech[kept_values].to_csv("data/report_appTech.csv", index=False)

    (app_tech_ds,app_tech_schema) = observations_for_df(df_name="data/report_appTech.csv",\
                                        df_format="csv",\
                                        df=app_tech)
    
    
    extra_delta_mapping = {
        "Intraday_Delta":["Adj Close","Open"]
    }

    app_tech_lineage = OutputDataLineage(schema=app_tech_schema,\
                                         input_schemas_mapping=OutputDataLineage.generate_direct_mapping(
                                             output_schema=app_tech_schema,\
                                             input_schemas=[
                                                 (all_assets_schema,extra_delta_mapping)
                                             ]
                                         ))
    
    print(json.dumps(app_tech_lineage))

    
    app_tech_lineage_exec = DataLineageExecution(lineage=app_tech_lineage,\
                                        application_execution=app_exec)
    
    print(json.dumps(app_tech_lineage_exec))

    
    app_tech_metric = DataMetrics(
        metrics=DataMetrics.extract_metrics_from_dataframe(df=app_tech),\
        schema=app_tech_schema,\
        lineage_execution=app_tech_lineage
    )

    print(json.dumps(app_tech_metric))

        
    (buzz_feed_ds,buzz_feed_schema) = observations_for_df(df_name="data/report_buzzfeed.csv",\
                                        df_format="csv",\
                                        df=buzz_feed)
    
    buzz_feed_lineage = OutputDataLineage(schema=buzz_feed_schema,\
                                         input_schemas_mapping=OutputDataLineage.generate_direct_mapping(
                                             output_schema=buzz_feed_schema,\
                                             input_schemas=[
                                                 (all_assets_schema,extra_delta_mapping)
                                             ]
                                         ))
    
    print(json.dumps(buzz_feed_lineage))

    
    buzz_feed_lineage_exec = DataLineageExecution(lineage=buzz_feed_lineage,\
                                        application_execution=app_exec)
    
    print(json.dumps(buzz_feed_lineage_exec))

    
    buzz_feed_metric = DataMetrics(
        metrics=DataMetrics.extract_metrics_from_dataframe(df=buzz_feed),\
        schema=buzz_feed_schema,\
        lineage_execution=buzz_feed_lineage
    )

    print(json.dumps(buzz_feed_metric))

    


if __name__=="__main__":
    main()