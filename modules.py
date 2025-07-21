import zipfile
from datetime import datetime
from calendar import monthrange
import os
import re
import psycopg2
import pickle
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, timedelta
import undetected_chromedriver as uc
import sys


def unzip_folder(zip_file_path, output_dir='data'):
    try:
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

        # Create output directory if it doesn't exist
        # output_path = f'{output_dir}/{timestamp}'
        output_path = output_dir
        os.makedirs(output_path, exist_ok=True)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(output_path)

        print(f'--Unzipped to: {output_path}')
        os.remove(zip_file_path)
        return output_path

    except Exception as e:
        print(f'Error: {e}')
        return None

def get_month_dates(year, month):
    _, last_day = monthrange(year, month)

    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, last_day)

    today = datetime.today()
    month_ = today.month
    first_dates = False

    if month_ == month:

        if today.day <= 2:

            if month_ == 1:
                start_date = datetime(today.year - 1, 12, 31)
            else:
                _,last_day = monthrange(year, month_ - 1)
                start_date = datetime(today.year, month_ - 1, last_day)

            end_date = today
            first_dates = True

        else:
            last_day = today.day - 1
            end_date = datetime(year, month, last_day)


    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    return first_dates, start_date_str, end_date_str

def get_cursor():
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    config_setting = 'DB SETTINGS'

    db_params = {
    'host': config.get(config_setting,'HOST'),
    'database': config.get(config_setting,'DBNAME'),
    'user': config.get(config_setting,'USER'),
    'password': config.get(config_setting,'PASS'),
    'port': config.get(config_setting,'PORT'),
    }
    
    connection = psycopg2.connect(**db_params)

    cursor = connection.cursor()

    return cursor,connection

def proxies(username, password, endpoint, port):
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Proxies",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
              },
              bypassList: ["localhost"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (endpoint, port, username, password)

    extension = 'proxies_extension.zip'

    with zipfile.ZipFile(extension, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return extension

def wait_visible(driver,xpath,time=5,handle_exception=False):
    try:
        element = WebDriverWait(driver,time).until(EC.presence_of_element_located((By.XPATH,xpath)))
        return  element
    except:
        return None

def wait_click(driver,xpath,time=5):
    WebDriverWait(driver,time).until(EC.element_to_be_clickable((By.XPATH,xpath))).click()

def create_cookies(driver,phone_):
    cookies = driver.get_cookies()
    
    pickle.dump(cookies, open(f'cookies/{phone_}.pkl', 'wb'))

def get_date_90_days_back():
    current_date = datetime.now()

    _date = current_date - timedelta(days=90)

    return _date.day ,_date.month ,_date.year

def current_date():
    current_date = datetime.now()

    return current_date.day - 1 , current_date.month

def get_current_date_formated():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')

def get_cookies(userID):
    try:
        cookies = pickle.load(open(f"cookies/{userID}.pkl","rb"))
        return cookies
    except Exception as e:
        print("Session not exists for the user ID: %s" % userID)
        return None

def init_driver(cookies=None,proxy=False,headless=False):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-notifications")
    
    if proxy:
        proxy_address = "in.smartproxy.com" # "dc.smartproxy.com"
        proxy_port = "10000"
        proxy_username = "sp82ne37op"
        proxy_password = "cWk2wxt3sQ1muvUJ6w"


        proxies_extension = proxies(proxy_username, proxy_password, proxy_address, proxy_port)
        chrome_options.add_extension(proxies_extension)

    if headless:
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--disable-gpu')  
        
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath('downloads'),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)
    # driver = uc.Chrome(driver_executable_path="chromedriver.exe",options=chrome_options)
    driver.get('https://partner.swiggy.com/login/')

    time.sleep(2)

    if cookies:
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except:
                continue
        
        print("\nCookies added.")
    

    print("Driver Initiated.\n")
    return driver
