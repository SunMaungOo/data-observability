import pandas as pd
import os
import json
from hashlib import md5
import git

def _default(self,obj):
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