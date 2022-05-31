from abc import ABC, abstractmethod
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import Dict,List,Optional,Any

import os, uuid, sys
import pandas as pd


@dataclass
class User:
    email:str
    password:str 


class DAO(ABC):
    """
    _summary_

    _extended_summary_

    :param ABC: _description_
    :type ABC: _type_
    """
    @abstractmethod
    def get_all(self)->DataFrame:pass

    @abstractmethod
    def get(self)->DataFrame:pass

    def get_latest_file(self)->DataFrame:pass

    @abstractmethod
    def update(self)->None:pass

    @abstractmethod
    def upsert(self)->None:pass 

    @abstractmethod
    def delete(self)->None:pass 

    @abstractmethod 
    def save(self)->None:pass

class AzureDatalakeDAO(DAO):
    """
    _summary_

    _extended_summary_

    :param DAO: _description_
    :type DAO: _type_
    """
    azure_user:User 

    def __init__(self, email:str, token:str)->None:pass 

    def create_file_system(self)->None:pass 


    def get_all(self)->DataFrame:pass

    def get_latest_file(self, file:str)->str:pass

    def save(self)->None:pass 

    def delete(self)->None:pass 

    def upsert(self)->None:pass 

    def update(self)->None:pass 

class PostgresDAO(DAO):
    """
    _summary_

    _extended_summary_

    :param DAO: _description_
    :type DAO: _type_
    """

    def __init__(self)->None:pass 

    def create_database(self)->None:pass 

    def create_table(self)->None:pass 

    def save(self)->None:pass 

    def delete(self)->None:pass 

    def insert(self)->None:pass 

    def update(self)->None:pass 

    def query(self)->None:pass 

    def get_all_databases(self)->None:pass

    def get_all_tables(self)->None:pass 






