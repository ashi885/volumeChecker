import json
# import requests

import time

import psycopg2
import pyotp

# import xlwings as xw

import sqlite3
# import os
import requests
from urllib.parse import parse_qs, urlparse
import sys
from fyers_apiv3 import fyersModel
# from fyers_apiv3 import accessToken
from datetime import datetime

import logging
from datetime import datetime
# from fyers_apiv3.Websocket import ws
from fyers_apiv3.FyersWebsocket import data_ws

grant_type = "authorization_code"                  ## The grant_type always has to be "authorization_code"
response_type = "code"                             ## The response_type always has to be "code"
state = "sample"                                   ##  The state field here acts as a session manager. you will be sent with the state field after successfull generation of auth_code

# fyersDWS


DB_HOST = '127.0.0.1'
DB_PORT = '5432'  # Default PostgreSQL port
DB_NAME = 'volumechecker'
DB_USER = 'postgres'
DB_PASSWORD = 'mysql'


log_path = 'data_ingestion/logs'

# db = sqlite3.connect("banknifty2024_data.db", check_same_thread=False)
# c = db.cursor()

APP_ID = "FU2R22RCQA"  # App ID from myapi dashboard is in the form appId-appType. Example - EGNI8CE27Q-100, In this code EGNI8CE27Q will be APP_ID and 100 will be the APP_TYPE
APP_TYPE = "100"
SECRET_KEY = 'G6X9B6TEOG'
client_id = f'{APP_ID}-{APP_TYPE}'

FY_ID = "XG08629"  # Your fyers ID
APP_ID_TYPE = "2"  # Keep default as 2, It denotes web login
TOTP_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXX"  # TOTP secret is generated when we enable 2Factor TOTP from myaccount portal
PIN = "XXXX"  # User pin for fyers account

### Connect to the sessionModel object here with the required input parameters



REDIRECT_URI = "https://127.0.0.1/"  # Redirect url from the app.

# API endpoints

BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api.fyers.in/api/v2"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"  # /send_login_otp_v2
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"
SUCCESS = 1
ERROR = -1

exchange = "NSE"
equity = "-EQ"
writeconn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
writecur = writeconn.cursor()

def send_login_otp(fy_id, app_id):
    try:
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json={"fy_id": fy_id, "app_id": app_id})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


def verify_totp(request_key, totp):
    try:
        result_string = requests.post(url=URL_VERIFY_TOTP, json={"request_key": request_key, "otp": totp})
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    except Exception as e:
        return [ERROR, e]


######################################################################################################################################
open_position = []
symbol = []

def getTime():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def login():
    # global fyers
    # global sheet
    global tickerlist
    global data_type
    global newtoken
    global reset

    logging.basicConfig(filename='data_ingestion/logs/realtime2024v1.log', encoding='utf-8', level=logging.INFO, filemode="a",
                        format='%(asctime)s %(message)s')
    logging.info("started")

    # session = accessToken.SessionModel(client_id=client_id, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,
    #
    #                                    response_type='code', grant_type='authorization_code')
    appSession = fyersModel.SessionModel(client_id=client_id, redirect_uri=REDIRECT_URI, response_type=response_type,
                                         state=state, secret_key=SECRET_KEY, grant_type=grant_type)

    # ## Make  a request to generate_authcode object this will return a login url which you need to open in your browser from where you can get the generated auth_code
    generateTokenUrl = appSession.generate_authcode()
    generateTokenUrl

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type=response_type,
        grant_type=grant_type
    )


    urlToActivate = session.generate_authcode()
    # print(f'URL to activate APP:  {urlToActivate}')
    logging.info(f'URL to activate APP:  {urlToActivate}')
    # Step 1 - Retrieve request_key from send_login_otp API

    send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)

    if send_otp_result[0] != SUCCESS:
        print(f"send_login_otp failure - {send_otp_result[1]}")
        logging.info(f"send_login_otp failure - {send_otp_result[1]}")
        sys.exit()
    else:
        print("send_login_otp success")
        logging.info("send_login_otp success")

    # Step 2 - Verify totp and get request key from verify_otp API
    for i in range(1, 3):
        request_key = send_otp_result[1]
        verify_totp_result = verify_totp(request_key=request_key, totp=pyotp.TOTP(TOTP_KEY).now())
        if verify_totp_result[0] != SUCCESS:
            # print(f"verify_totp_result failure - {verify_totp_result[1]}")
            logging.info(f"verify_totp_result failure - {verify_totp_result[1]}")
            time.sleep(1)
        else:
            # print(f"verify_totp_result success {verify_totp_result}")
            logging.info(f"verify_totp_result success {verify_totp_result}")
            break

    request_key_2 = verify_totp_result[1]

    # Step 3 - Verify pin and send back access token
    ses = requests.Session()
    payload_pin = {"request_key": f"{request_key_2}", "identity_type": "pin", "identifier": f"{PIN}",
                   "recaptcha_token": ""}
    res_pin = ses.post('https://api-t2.fyers.in/vagator/v2/verify_pin', json=payload_pin).json()
    # print(res_pin['data'])
    logging.info(res_pin['data'])
    ses.headers.update({
        'authorization': f"Bearer {res_pin['data']['access_token']}"
    })

    authParam = {"fyers_id": FY_ID, "app_id": APP_ID, "redirect_uri": REDIRECT_URI, "appType": APP_TYPE,
                 "code_challenge": "", "state": "None", "scope": "", "nonce": "", "response_type": "code",
                 "create_cookie": True}
    authres = ses.post('https://api.fyers.in/api/v2/token', json=authParam).json()
    # print(authres)
    logging.info(authres)
    url = authres['Url']
    # print(url)
    logging.info(f"URL is {url}")
    parsed = urlparse(url)
    auth_code = parse_qs(parsed.query)['auth_code'][0]
    session.set_token(auth_code)
    response = session.generate_token()
    access_token = response["access_token"]
    # print(access_token)
    logging.info(f"Access token is {access_token}")
    fyers = fyersModel.FyersModel(client_id=client_id, token=access_token, log_path=log_path)
    # print(fyers.get_profile())
    fyers.token = access_token
    newtoken = f"{client_id}:{access_token}"
    print("TEST")
    # access_token = "SPXXXXE7-100:eyJ0eXAiOiJK***.eyJpc3MiOiJhcGkuZnllcnM***.IzcuRxg4tnXiULCx3***"
    #run_process_symbol_data(newtoken)

    # print(fyers.get_profile())


    # symbol = ['NSE:NIFTYBANK-INDEX', 'NSE:INDIAVIX-INDEX']
    # data = {
    #     "symbols": "NSE:SBIN-EQ,NSE:NIFTY23O1219600PE,NSE:NIFTYBANK-INDEX"
    # }
    #
    # response = fyers.quotes(data=data)
    # response
    # print(response)
    # sheet1 = xw.Book("Scanner/BANKNIFTYVER4.xlsx").sheets[0]
    # tickerlist = sheet.range("A4").expand("down").value
    reset = 0
    symbol = []
    # for i in tickerlist:
    #
    #     if i == None:
    #         break
    #     print(i)
    #     i = i.upper()
    #     symbol.append(f"{exchange}:{i}")
    #


# sheet1 = xw.Book("Scanner/BANKNIFTYVER4.xlsx").sheets[0]

def get_symbols(exchange, security):
    logging.info(f"Getting Data")
    for l in range(4, 15):
        # if sheet1.range("A" + str(l)).value is not None:
            # logging.info("excel value is " + sheet.range("F" + str(l)).value)
            # security = sheet1.range("A" + str(l)).value
            symbol.append(f"{exchange}:{security}")
            # symbol.append(f"{security.strip()}")

        # symbol.append(f"{exchange}:{i}")
        #  print(f"{exchange}:{i}{equity}")

        # print(symbol)
        # symbol.append(f"NSE:INDIAVIX-INDEX")
        # symbol.append(f"NSE:NIFTYBANK-INDEX")
    logging.info(f"Symbols new {symbol}")
    # # #
    # #
    # symbol = ["NSE:SBIN-EQ","NSE:INFY-EQ"]
    print(symbol)

def insert_realtime_data(symbol, volume):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        symbol = symbol.replace('NSE:', '').replace('-EQ', '')
        # Prepare the data tuple
        data = (symbol, volume)
        # symbol = symbol.replace('NSE:', '').replace('-EQ', '')
        # Insert data into the volchecker table
        insert_query = """
            INSERT INTO volchecker (symbol, current_volume)
            VALUES (%s, %s)
            ON CONFLICT (symbol) 
            DO UPDATE SET current_volume = EXCLUDED.current_volume;
        """
        cur.execute(insert_query, data)

        # Commit the changes
        conn.commit()
        print(f"Inserted or updated data for symbol: {symbol}")

        # Close the cursor and connection
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error inserting data: {e}")
def onmessage(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.
    """
    try:
        print("onmessage")
        voltraded = message['vol_traded_today']
        ltp = message['ltp']
        script = message['symbol'][4:]
        print(script, ltp, voltraded)

        scriptname = script.replace('NSE:', '').replace('-EQ', '')

        data = (scriptname, voltraded)
        print(data)

        # Insert data into the volchecker table
        writeinsert_query = """
            INSERT INTO stock_data.volchecker (symbol, current_volume)
            VALUES (%s, %s)
            ON CONFLICT (symbol)
            DO UPDATE SET current_volume = EXCLUDED.current_volume;
        """

        # Execute the query
        writecur.execute(writeinsert_query, data)
        writeconn.commit()
        print("Inserted:", data)

    except Exception as e:
        # Print the error and rollback the transaction
        print("Error:", e)
        writeconn.rollback()


def onerror(message):
    """
    Callback function to handle WebSocket errors.

    Parameters:
        message (dict): The error message received from the WebSocket.


    """
    print("Error:", message)


def onclose(message):
    """
    Callback function to handle WebSocket connection close events.
    """
    print("Connection closed:", message)


def onopen():
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.

    """
    # Specify the data type and symbols you want to subscribe to
    print("open")
    data_type = "SymbolUpdate"

    # Subscribe to the specified symbols and data type
    # symbol = ['NSE:NIFTYBANK-INDEX', 'NSE:INDIAVIX-INDEX']
    # data = {
    #     "symbols": "NSE:SBIN-EQ,NSE:NIFTY23O1219600PE,NSE:NIFTYBANK-INDEX"
    # }
    print(symbol)
    fyersDWS.subscribe(symbols=symbol, data_type=data_type)

    # Keep the socket running to receive real-time data
    fyersDWS.keep_running()

# def create_tables():
#     print("Started")
#     # c.execute("CREATE TABLE IF NOT EXISTS data (ts datetime primary key, security Text, price real(15,5),high real(15,5),open real(5,15), volume integer)")
#     c.execute(
#         "CREATE TABLE  IF NOT EXISTS data (uid Text primary key, security Text, price real(15,5), ts datetime)")
#     try:
#         db.commit()
#         print("table_created")
#     except:
#         db.rollback()

# def get_marketdata():


def get_symbols_from_volchecker():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Query to fetch symbols from the volchecker table
        query = "SELECT symbol FROM stock_data.volchecker"
        cur.execute(query)

        # Fetch all rows and extract the symbol from each row
        rows = cur.fetchall()
        symbols = [f"NSE:{row[0]}-EQ" for row in rows]  # Extract symbols from the rows

        # Close the cursor and connection
        cur.close()
        conn.close()

        return symbols

    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

def getTime():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":
    global fyersDWS
    print("starting")
    # create_tables()
    print("log in")
    login()
    print("getting symbols")
    # get_symbols()
    # symbol = ["NSE:SBIN-EQ", "NSE:INFY-EQ"]
    symbol = get_symbols_from_volchecker()
    print(symbol)
    print("=============================================================")
    fyersDWS = data_ws.FyersDataSocket(
        access_token=newtoken,  # Access token in the format "appid:accesstoken"
        log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=False,  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,  # Callback function to subscribe to data upon connection.
        on_close=onclose,  # Callback function to handle WebSocket connection close events.
        on_error=onerror,  # Callback function to handle WebSocket errors.
        on_message=onmessage  # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyersDWS.connect()



# Function to insert real-time volume data into PostgreSQL

