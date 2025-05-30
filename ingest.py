import pandas as pd
import os
import json
from hashlib import md5
import git
from typing import List,Tuple
from functools import reduce

def _default(obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)

_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default


class Application:
    """
    Application in static space
    """
    def __init__(self,name:str):
        # primary key
        self.name = name
        self.id = md5(name.encode("utf-8")).hexdigest()

    @staticmethod
    def fetch_file_name():
        application_name = os.path.basename(
            os.path.realpath(__file__)
        )

        return application_name

    def to_json(self):
        return {
            "id":self.id,
            "name":self.name
        }


class ApplicationRepository:
    """
    Location of application in static space
    """
    def __init__(self,location:str,application:Application):
        self.location = location
        self.application = application

        id_content = ",".join([location,application.id])

        self.id = md5(id_content.encode("utf-8")).hexdigest()

    @staticmethod
    def fetch_git_location()->str:
        code_repo = git.Repo(os.getcwd(),
                             search_parent_directories=True).remote().url
        return code_repo

    def to_json(self):
        return {
            "id":self.id,
            "locaion":self.location,
            "application":self.application.id
        }

class DataSource:
    def __init__(self,location:str,format:str=None):
        self.location = location
        self.format = format
        id_content = ",".join([location,format])

        self.id = md5(id_content.encode("utf-8")).hexdigest()

    def to_json(self):
        return {
            "id":self.id,
            "location":self.location,
            "format":self.format
        }

class Schema:
    def __init__(self,fields:List[Tuple[str,str]],data_source:DataSource):
        self.fields = fields
        self.data_source = data_source
        
        linearized_fields = ",".join(
            list(
                map(lambda x: x[0]+"-"+x[1],
                    sorted(fields))
            )
        )

        id_content = ",".join([linearized_fields,data_source.id])

        self.id = md5(id_content.encode("utf-8")).hexdigest()

    def to_json(self):
        fields = reduce(
            lambda x,y: dict(**x,**y),
            map(
                lambda f: {f[0]:f[1]},
                self.fields
            )
        )
        return {
            "id":self.id,
            "fields":fields,
            "data_source":self.data_source.id
        }

    @staticmethod
    def extract_fields_from_dataframe(df:pd.DataFrame)->List[Tuple[str,str]]:
        fields = list(zip(
            df.columns.values.tolist(),
            map(
                lambda x: str(x),
                df.dtypes.values.tolist()
            )
        ))

        return fields

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