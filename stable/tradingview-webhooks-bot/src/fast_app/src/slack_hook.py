import json
import re
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import requests
alerts_slack_url= "https://hooks.zapier.com/hooks/catch/10447300/3x3io45/"
def main():
    try:
        slack_msg = f""":red_circle: AWS Alert.
                *Subject*: test 
                *Alarm Name*: test2
                *Alarm Reason*: test 
                """

        data = jsonable_encoder({"text":slack_msg})
       

        resp = requests.post(url=alerts_slack_url,data=data)
        print(f"resp: {resp}  ")
        print(f"data: {data}")
        print(f"slack_msg: {slack_msg}")
    except Exception as e:
        print(f"Error in bracket_trade_fill: {e}")
if __name__ == "__main__":
    main()

    