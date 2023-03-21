## Introduction


This repository is a customized version of [Interactive Brokers Gateway Docker](https://github.com/UnusualAlpha/ib-gateway-docker) , forked to meet specific requirements for educational materials.

## Interactive Brokers Gateway Docker

Interactive Brokers Gateway Docker is a docker image that enables the running of the Interactive Brokers Gateway Application without any human interaction on a docker container.

The original TVWB can be found on GitHub at [Interactive Brokers Gateway Docker](https://github.com/UnusualAlpha/ib-gateway-docker)


## What is this repository?

This repository allows for the quick launch of TradingBoat using Docker. TradingBoat is a trading platform that receives orders from TradingView Webhook and places them into IB Gateway or TWS. The platform can use NGROK, Flask(TVWB), Redis, TBOT, and IB Gateway.

Some environment variables used for the docker are inherited from Interactive Brokers Gateway Docker. It also includes its own environment variables that control other components such as NGROK, Flask, Redis, and TBOT.

![TradingBoat-Docker](https://user-images.githubusercontent.com/1986788/226738416-4fe3275b-e116-4f6e-9372-0aea9f4ee9fd.png)

 

## How to Run Tbot on TradingBoat Docker

### How to Build Docker

Once you have obtained access permission to https://github.com/PlusGenie/tbot-tradingboat , follow the steps below:

To build Docker, follow the steps below after obtaining access permission to https://github.com/PlusGenie/tbot-tradingboat :

Clone the repository:

```
git clone https://github.com/PlusGenie/ib-gateway-docker
```


Copy dotenv into the root directory:

```
cd ib-gateway-docker
cp stable/tbot/dotenv .env
```

Open the dotenv file using a text editor and update the values of:

```
TWS_USERID: The account name used to log in to TWS / IB Gateway

TWS_PASSWORD: The password used to log in to TWS / IB Gateway

VNC_SERVER_PASSWORD

NGROK_AUTH: Authentication Token for NGROK as needed. (optional)
```


Copy your GitHub SSH key into the config directory in order to access:

```
copy id_rsa stable/tbot
copy id_rsa_pub stable/tbot
```


Once these steps are complete, you can run Tbot on Tradingboat Docker using the following command:

```
docker-compose up --build
```


After the docker starts, you can use two interfaces. Firstly, use VNC Viewer to access IB Gateway. Secondly, use the web application to track orders from TradingView to Interactive Brokers.


### How to Access IB Gateway through VNC server

Please use VNC Viewer to access the IB Gateway via the VNC server.

VNC Server: 127.0.0.1:5900


![VNC_Viewer](https://user-images.githubusercontent.com/1986788/226739017-c6f15476-2960-4d4e-a334-8d6b8892dc7c.png)

![VNC_Viwer_IB](https://user-images.githubusercontent.com/1986788/226739107-183ccada-b605-4e13-82a2-56209933c0c4.png)


### How to Access Web App and Ngrok

After running docker-compose successfully, you can access the web application using a browser. The Dockerfile pulls the source of the web application from https://github.com/PlusGenie/tradingview-webhooks-bot . 

Then go to http://127.0.0.1:5000

![WEBAPP_TVWB_TBOT_DECODER](https://user-images.githubusercontent.com/1986788/226739163-9b8fa027-fbeb-486e-9ca2-fa6aadf28fb2.png)



