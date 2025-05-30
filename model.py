import pandas as pd
import os
import json
from hashlib import md5
import git
from typing import List,Tuple
from functools import reduce
from math import isnan
from numbers import Number

def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = json.JSONEncoder().default
json.JSONEncoder.default = _default
pd.options.mode.chained_assignment = None



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
    
class DataMetrics:

    def __init__(self,metrics:List[Tuple[str,float]],schema:Schema):

        self.metrics = metrics
        self.schema = schema

        id_content = ",".join([schema.id])

        self.id = md5(id_content.encode("utf-8")).hexdigest()
    
    def to_json(self):
        fields = reduce(
            lambda x,y:dict(**x,**y),
            map(
                lambda f: {f[0]:f[1]}
            ,self.metrics)
        )

        return {
            "id":self.id,
            "metrics":fields,
            "schema":self.schema.id
        }
    
    @staticmethod
    def extract_metrics_from_dataframe(df:pd.DataFrame)->List[Tuple[str,float]]:
        
        describe_value = df.describe(include="all")
        
        metrics = {}

        empty_nan_filter = lambda x: isinstance(x[1],Number) and not isnan(x[1])

        for field in describe_value.columns[1:]:

            field_dict = describe_value[field].to_dict()

            mapped_items = map(lambda x: (field + "." + x[0], x[1]), field_dict.items())

            filtered_items = filter(empty_nan_filter, mapped_items)

            metric_description = dict(filtered_items)

            metrics.update(metric_description)

        return list(metrics.items())
    
class OutputDataLineage:
    def __init__(self,schema:Schema,input_schemas_mapping:List[Tuple[Schema,dict]]):
        self.schema = schema
        self.input_schemas_mapping = input_schemas_mapping

        id_content = ",".join([schema.id,self.linearzie()])

        self.id = md5(id_content.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_direct_mapping(output_schema: Schema, input_schemas: list[Schema])->List[Tuple[Schema,dict]]:

        input_schemas_mapping = list()

        output_schema_field_names = [field[0] for field in output_schema.fields]

        for schema in input_schemas:

            mapping = dict()

            for field in schema.fields:
                # if field name is found
                if field[0] in output_schema_field_names:
                    mapping[field[0]] = [field[0]]
            
            if len(mapping)>0:
                input_schemas_mapping.append((schema,mapping))
        
        return input_schemas_mapping



    def to_json(self):
        return {
            "id":self.id,
            "schema":self.schema.id,
            "input_schemas_mapping":self.input_schemas_mapping
        }


    def linearzie(self)->str:
        linearized = ""

        for schema in self.input_schemas_mapping:
            for key in schema[1]:
                linearized += key
                linearized += ":"
                linearized += "-".join(schema[1][key]) + ","
        
        linearized = linearized[:-1]

        return linearized

def observations_for_df(df_name: str, df_format: str, df: pd.DataFrame,is_print_observation=True)->Tuple[DataSource,Schema,DataMetrics]:

    data_source = DataSource(location=df_name,format=df_format)

    schema = Schema(fields=Schema.extract_fields_from_dataframe(df=df),\
                    data_source=data_source)
    
    metric = DataMetrics(metrics=DataMetrics.extract_metrics_from_dataframe(df=df),\
                         schema=schema)
    
    if is_print_observation:
        print(json.dumps(data_source))
        print(json.dumps(schema))
        print(json.dumps(metric))





    return (data_source,schema,metric)