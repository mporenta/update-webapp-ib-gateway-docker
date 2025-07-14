import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, String, Integer, Float, BigInteger,
    PrimaryKeyConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
load_dotenv()
boof = os.getenv("HELLO_MSG")
api_key = os.getenv("POLYGON_API_KEY")
from polygon import RESTClient

from polygon.rest.models import TickerDetails
from polygon.rest.reference  import *
from polygon.exceptions import BadResponse
API_KEY   = os.getenv("POLYGON_API_KEY")
poly_bad_response = BadResponse()
client = RESTClient(api_key)