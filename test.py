from modules import *
import os
import pandas as pd
import requests
import warnings
import io
import logging
import shutil
import json
import traceback
from datetime import datetime

warnings.filterwarnings('ignore')
current_date = datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(filename = f"logs/swiggy_logs_{current_date}.log", 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

cur,connection = get_cursor()
ADS_DATA_NOT_AVAILABLE = False

### ************ MODULES ************ ###

def printLog(msg,statement=None,end=None):
    """
    Prints log messages.

    Args:
        message (str): Message to be logged.
    """
    logging.info(msg)

    if end is not None:
        print(msg, end = end)
        return
    if statement is None:
        print(msg)
    else:
        print(msg,statement)

def printError(e,print_=False):
 
    error_type = type(e).__name__
    line_number = sys.exc_info()[-1].tb_lineno
    if e.args:
        error_name = e.args[0]
    else:
        error_name = "No additional information available"
    error_msg = f"Error Type: {error_type}\nError Name: {error_name}\nLine where error occurred: {line_number}"
 
    logging.error(error_msg)

    if print_:
        print(error_msg)

def execute_query(query,values):
    """
    Executes a given SQL query with provided values.

    Args:
        query (str): SQL query to be executed.
        values (tuple): Values to be inserted into the query.
    """
    try:
        cur.execute(query, values)
    except Exception as e:
        printError(e,True)
        connection.commit()

### ************ ADS PERFORMANCE ************ ###
def refresh(driver,frame=0):
    driver.switch_to.default_content()

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
    driver.switch_to.frame(iframe)
    
    if frame == 1:
        frames = driver.find_elements(By.TAG_NAME,'iframe')
        driver.switch_to.frame(frames[0])
        
    elif frame == 2:
        frames = driver.find_elements(By.TAG_NAME,'iframe')
        driver.switch_to.frame(frames[1])
    
    return
    
def select_date(driver,date_to_select):

    def func():
        nonlocal elements
        nonlocal driver

        for input_element in elements:
            try:
                input_element.click()
                time.sleep(1.5)
                element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')

                while True:    
                    if element is None:
                        input_element.click()
                        time.sleep(1.5)
                        element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')
                        
                    selected_date_text = element.text
                    selected_date = datetime.strptime(selected_date_text, '%B %Y')
                    selected_month = selected_date.month
                    selected_year = selected_date.year

                    if provided_month == selected_month and provided_year == selected_year:
                        xpath = f"//*[contains(@class, 'DayPicker-Day') and text()='{day}']"
                        wait_click(driver,xpath)
                        time.sleep(1.5)
                        break

                    if provided_year < selected_year or (provided_year == selected_year and provided_month < selected_month):
                        wait_visible(driver, '//span[@aria-label="Previous Month"]').click()
                    else:
                        wait_visible(driver, '//span[@aria-label="Next Month"]').click()
                
                time.sleep(2)

            except Exception as e:
                return False
        
        return True

    driver.switch_to.default_content()

    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    time.sleep(3)

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]',15)
    driver.switch_to.frame(iframe)
    
    provided_date = datetime.strptime(date_to_select, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    input_element = wait_visible(driver,"//input[@placeholder='To']")

    from_input_element = wait_visible(driver,"//input[@placeholder='From']")

    elements = [input_element,from_input_element]

    check = func()

    start = input_element.get_attribute('value')
    end = from_input_element.get_attribute('value')

    if check:
        if start == end:
            printLog(f"Date selected: {date_to_select},", end= " ")
            return True
        else:
            printLog(f"Date selection error: {date_to_select}")
            return False
    else:
        printLog(f"Date selection error: {date_to_select}")
        return False
  
def get_data(driver,date_):

    def wait(frame=0):
        global ADS_DATA_NOT_AVAILABLE
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.XPATH,'//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            error_in_frame = "Couldn't load the data for this visual"
            if error_in_frame in soup.text:
                printLog(error_in_frame)
                ADS_DATA_NOT_AVAILABLE = True
                return False
            cells = soup.find_all('div', class_='pivotTableCellWrap')

            if len(cells) > 0:
                break
            elif time_lapsed >= 20:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
                    
    driver.switch_to.default_content()

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
    driver.switch_to.frame(iframe)

    try:
        wait_click(driver,'//*[text()="New Users"]',5)
    except:
        return False
    data_available = wait(0)

    if not data_available:
        printLog("Data not fetched from 1st pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    data = {}

    for i in range(0, len(cells), 7):  # Assuming each record has 7 cells
        try:
            res_id = re.search(r'\d+',cells[i].text).group()
            record = {
                'ad_date' : date_,
                'restaurant_id': res_id,
                'created_at' : get_current_date_formated(),
                'updated_at' : get_current_date_formated(),
                'ad_spend': cells[i+2].text.strip(),
                'impressions': cells[i+3].text.strip(),
                'menu_visits': cells[i+4].text.strip(),
                'orders': cells[i+5].text.strip(),
                'sales': cells[i+6].text.strip(),
            }
        except:
            continue
        data[res_id] = record

    if len(data) == 0:
        printLog("No data available.")
        driver.switch_to.default_content()
        try:
            wait_click(driver,'/html/body/div[2]/div/div/button')
        except:
            pass
        return True

    data_available = wait(1)

    if not data_available:
        printLog("Data not fetched from 2nd pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
    length = len(headers)
    
    try:
        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'nu_orders':dct.get('NU Orders','0.0'),
                    }
            
            data[res_id].update(record)
        
        for res,record in data.items():
            if 'nu_orders' in data[res].keys():
                pass
            else:
                data[res]['nu_orders'] = '0.0'
            
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['nu_orders'] = '0.0'
    

    driver.switch_to.default_content()
    driver.switch_to.frame(iframe)

    wait_click(driver,'//*[text()="Slot Types"]')

    data_available = wait(1)

    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')
    scraped_res = []
    try:
        length = int(cells[-1]['aria-colindex'])
        headers = [cells[i].text for i in range(length)]

        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')

            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'breakfast_orders':dct.get('Breakfast','0.0'),
                    'dinner_orders':dct.get('Dinner','0.0'),
                    'ln1_orders':   dct.get('LN 1 (12-5 am)','0.0'),
                    'ln2_orders':   dct.get('LN 2 (11-12 pm)','0.0'),
                    'lunch_orders': dct.get('Lunch','0.0'),
                    'snacks_orders':dct.get('Snacks','0.0'),
                    }
            
            data[res_id].update(record)
            scraped_res.append(res_id)

        for res in data.keys():
            if res not in scraped_res:
                data[res]['breakfast_orders'] = '0.0'
                data[res]['dinner_orders'] = '0.0'
                data[res]['ln1_orders'] = '0.0'
                data[res]['ln2_orders'] = '0.0'
                data[res]['lunch_orders'] = '0.0'
                data[res]['snacks_orders'] = '0.0'
            
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['breakfast_orders'] = '0.0'
            data[res]['dinner_orders'] = '0.0'
            data[res]['ln1_orders'] = '0.0'
            data[res]['ln2_orders'] = '0.0'
            data[res]['lunch_orders'] = '0.0'
            data[res]['snacks_orders'] = '0.0'
            
    for res,record in data.items():
        for key, value in record.items():
            if key not in ['restaurant_id', 'location', 'res_id']:
                data[res][key] = value.replace('₹','').replace(',', '').replace('%', '')
                
            if value == '':
                data[res][key] = '0.0'
      
    for res,record in data.items():

        query = """ 
        INSERT INTO swiggy_ad_metrics (ad_date, restaurant_id, created_at, updated_at, ad_spend, impressions, menu_visits, orders, sales, nu_orders, breakfast_orders,dinner_orders, ln1_orders, ln2_orders, lunch_orders, snacks_orders)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
        """
        
        execute_query(query,tuple(record.values()))
    
    connection.commit()
    printLog("--data scraped.")

    driver.switch_to.default_content()
    driver.refresh()

    return True
   
def process_ad_performance(driver,restaurant_ids):
    """
    Scrapes ad performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape ad performance for.
    """
    global ADS_DATA_NOT_AVAILABLE
    ADS_DATA_NOT_AVAILABLE = False

    driver.get(f'https://partner.swiggy.com/business-metrics/ads-performance/restaurant/{restaurant_ids[0]}')
    
    time.sleep(3)
    
    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    query = """
        SELECT DISTINCT date
        FROM swiggy_ads_month_track
        WHERE restaurant_id IN %(restaurant_list)s;
        """
    
    cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})

    record = cur.fetchone()

    if record is None:
        printLog("--Record not available. Run Process Ads!!!")
        
        for restaurant_id in restaurant_ids:
            query = """
                INSERT INTO swiggy_ads_month_track(
                restaurant_id,
                date
                ) 
                VALUES(
                    %s, %s
                    );
                """
            values = (restaurant_id,datetime.today().strftime('%Y-%m-%d'))
            execute_query(query,values)
        connection.commit()

        for i in range(31,1,-1):
            
            delta = datetime.today() - timedelta(days=i)
            date = delta.strftime('%Y-%m-%d')

            check = select_date(driver,date)
            if not check:
                driver.switch_to.default_content()
                continue

            try:
                refresh(driver)
                wait_click(driver,'//*[text()="Outlet level Performance"]',2)
            except Exception as e:
                printLog("Path is changed or Webpage structure is changed. Please check the code.")
                printError(e)
                pass
            
            get_data(driver,date)
            
            for restaurant_id in restaurant_ids:
                query = """
                        UPDATE swiggy_ads_month_track
                        SET 
                            date = %s
                        WHERE 
                            restaurant_id = %s
                        """
                values = (date, restaurant_id)
                execute_query(query,values)
            connection.commit()

            time.sleep(1)
    
    else:
        printLog("--Record available. Downloading new data...")
        previous_date = record[0]
        current_date = datetime.today().date()
        delta_days = (current_date - previous_date).days

        if delta_days > 30:
            printLog("--days to scraped are more than 30. Please run process ads file.")
            return
        
        ids_str = ','.join(map(str, restaurant_ids))
        cur.execute(f"select distinct ad_date from swiggy_ad_metrics where restaurant_id in ({ids_str})")
        records = cur.fetchall()
        scraped_dates = [str(record[0]) for record in records]

        for i in range(delta_days, 0, -1):
            delta = current_date - timedelta(days=i)
            date = delta.strftime('%Y-%m-%d')

            if date in scraped_dates:
                printLog("Date is already scraped. Skipping date.")
                continue
            
            check = select_date(driver,date)
            if not check:
                printLog("Date selection error. !!!SKIPPING DATE!!!")
                with open('ad_dates_with_error.txt','+a') as file:
                    file.write(date)
                    file.write('\n')
                continue

            try:
                refresh(driver)
                wait_click(driver, '//*[text()="Outlet level Performance"]', 2)
            except Exception as e:
                printLog("Path is changed or Webpage structure is changed. Please check the code.")
                printError(e)
                continue

            check = get_data(driver,date)
            if check:
                pass
            elif ADS_DATA_NOT_AVAILABLE:
                printLog("Unable to fetch data. Not available on Swiggy.")

                for restaurant_id in restaurant_ids:
                    query = """
                            UPDATE swiggy_ads_month_track
                            SET 
                                date = %s
                            WHERE 
                                restaurant_id = %s
                            """
                    values = (date, restaurant_id)
                    execute_query(query,values)
                connection.commit()
                return
            
            else:
                driver.refresh()
                select_date(driver,date)
                get_data(driver,date)

            for restaurant_id in restaurant_ids:
                query = """
                    UPDATE swiggy_ads_month_track
                    SET 
                        date = %s
                    WHERE 
                        restaurant_id = %s
                    """
                values = (date, restaurant_id)
                execute_query(query, values)
            connection.commit()

            time.sleep(1)
        
        return

### ************ DISCOUNT PERFORMANCE ************ ###
def get_rev_single_res(driver,restaurant_id,date):
    
    def wait(frame=0):
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.TAG_NAME,'iframe')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            card = soup.find('div', class_='card')

            if card is not None:
                break
            elif time_lapsed >= 20:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
      
    data = {'business_date': date,
            'created_at' : get_current_date_formated(),
            'updated_at' : get_current_date_formated(),
            'restaurant_id': restaurant_id,}

    data_available = wait(0)
    if not data_available:
        return
    
    soup = BeautifulSoup(driver.page_source)

    card = soup.find('div', class_='card')

    if card:
        elements = card.find_all('div', class_='cardItemContainer')
        for element in elements:
            title = element.find('div', class_='details').get_text(strip=True)
            value = element.find('div', class_='caption').get_text(strip=True)
            if 'Blank' in value:
                value = 0
            if title == "Orders":
                data['orders'] = value
            elif title == "Revenue":
                data['revenue'] = value
            elif title == "Spends":
                data['spends'] = value
            elif title == "AOV":
                data['aov'] = value

    refresh(driver,frame=2)
    
    soup = BeautifulSoup(driver.page_source)

    card = soup.find('div', class_='card')

    if card:
        elements = card.find_all('div', class_='cardItemContainer')
        for element in elements:
            title = element.find('div', class_='details').get_text(strip=True)
            value = element.find('div', class_='caption').get_text(strip=True)
            if 'Blank' in value:
                value = 0
            if title == "New User Orders":
                data['new_user_orders'] = value
            elif title == "Repeat User Orders":
                data['repeat_user_orders'] = value
            elif title == "Dormant User Orders":
                data['dormant_user_orders'] = value
    else:
        data['new_user_orders'] = 0
        data['repeat_user_orders'] = 0
        data['dormant_user_orders'] = 0
    
    for key, value in data.items():
        if key not in ['restaurant_id','business_date','created_at','updated_at']:
            try:
                data[key] = value.replace('₹','').replace(',', '').replace('%', '')
            except:
                continue

    if len(data.keys()) > 5:
        
        query = """ 
        INSERT INTO swiggy_discount_metrics (business_date, created_at, updated_at, restaurant_id, orders, revenue,
        spends, aov, new_user_orders, repeat_user_orders, dormant_user_orders)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        execute_query(query,tuple(data.values()))

        connection.commit()

        printLog("--data scraped.")
        driver.switch_to.default_content()

        return True
    
    else:
        printLog("--No data available")
        return True

def are_dates_same(date_str1, date_str2):
    # Define possible date formats
    date_formats = [
        "%Y-%m-%d",     # Example: 2024-06-10
        "%d-%m-%Y",     # Example: 10-06-2024
        "%d/%m/%Y",     # Example: 06/10/2024
        "%Y/%m/%d",     # Example: 2024/06/10
        "%B %d, %Y",    # Example: June 10, 2024
        "%d %B %Y",     # Example: 10 June 2024
        "%b %d, %Y",    # Example: Jun 10, 2024
        "%d %b %Y",     # Example: 10 Jun 2024
        "%Y-%m-%dT%H:%M:%S", # Example: 2024-06-10T15:30:00
        "%Y-%m-%d %H:%M:%S"  # Example: 2024-06-10 15:30:00
        # Add more formats as needed
    ]
    
    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                continue
        raise ValueError(f"Date format for '{date_str}' is not recognized")
    
    # Parse the dates
    date1 = parse_date(date_str1)
    date2 = parse_date(date_str2)
    
    # Compare the dates
    return date1.date() == date2.date()

def select_date(driver,date_to_select):

    def func():
        nonlocal elements
        nonlocal driver

        for input_element in elements:
            try:
                input_element.click()
                time.sleep(1.5)
                element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')

                while True:    
                    if element is None:
                        input_element.click()
                        time.sleep(1.5)
                        element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')
                        
                    selected_date_text = element.text
                    selected_date = datetime.strptime(selected_date_text, '%B %Y')
                    selected_month = selected_date.month
                    selected_year = selected_date.year

                    if provided_month == selected_month and provided_year == selected_year:
                        xpath = f"//*[contains(@class, 'DayPicker-Day') and text()='{day}']"
                        wait_click(driver,xpath)
                        time.sleep(1.5)
                        break

                    if provided_year < selected_year or (provided_year == selected_year and provided_month < selected_month):
                        wait_visible(driver, '//span[@aria-label="Previous Month"]').click()
                    else:
                        wait_visible(driver, '//span[@aria-label="Next Month"]').click()
                
                time.sleep(2)

            except Exception as e:
                return False
        
        return True

    driver.switch_to.default_content()

    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    time.sleep(3)

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]',15)
    driver.switch_to.frame(iframe)
    
    provided_date = datetime.strptime(date_to_select, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    input_element = wait_visible(driver,"//input[@placeholder='To']")

    from_input_element = wait_visible(driver,"//input[@placeholder='From']")

    elements = [input_element,from_input_element]

    check = func()

    start = input_element.get_attribute('value')
    end = from_input_element.get_attribute('value')

    if check:
        if are_dates_same(start, end):
            printLog(f"Date selected: {date_to_select},", end= " ")
            return True
        else:
            printLog(f"Date selection error: {date_to_select}")
            return False
    else:
        printLog(f"Date selection error: {date_to_select}")
        return False
  
def get_data_rev(driver,date):
        
    def wait(frame=0):
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.XPATH,'//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            cells = soup.find_all('div', class_='pivotTableCellWrap')

            if len(cells) > 0:
                break
            elif time_lapsed >= 20:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
      
    data = {}
 
    data_available = wait(0)
    if not data_available:
        printLog("Data not fetched from 1st pane.")
        return False
    
    refresh(driver,frame=1)
    
    soup = BeautifulSoup(driver.page_source)
    
    cells = soup.find_all('div', class_='pivotTableCellWrap')

    try:
        headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
        length = len(headers)

        data = {}
        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'business_date' : date,
                    'created_at' : get_current_date_formated(),
                    'updated_at' : get_current_date_formated(),
                    'restaurant_id': res_id,
                    'orders': dct.get('Orders','0'),
                    'revenue': dct.get('Revenue','0.0'),
                    'spends': dct.get('Spends','0.0'),
                    'aov' : dct.get('AOV','0.0')
                    }
            
            data[res_id] = record

    except Exception as e:
        printError(e)
        return False
    
    if len(data) == 0:
        printLog("No data available.")
        driver.switch_to.default_content()
        try:
            wait_click(driver,'/html/body/div[2]/div/div/button')
        except:
            pass
        return True
    
    data_available = wait(1)

    refresh(driver,frame=2)

    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    if len(cells) == 0:
        refresh(driver,frame=2)

    try:
        headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
        length = len(headers)

        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'new_user_orders': dct.get('New user orders','0'),
                    'repeat_user_orders': dct.get('Repeat user orders','0'),
                    'dormant_user_orders': dct.get('Dormant  user orders','0'),
                    }
            
            data[res_id].update(record)
        
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['new_user_orders'] = '0'
            data[res]['repeat_user_orders'] = '0'
            data[res]['dormant_user_orders'] = '0'

    for res,record in data.items():
        for key, value in record.items():
            if key not in ['restaurant_id','res_id']:
                data[res][key] = value.replace('₹','').replace(',', '').replace('%', '')
                
            if value == '':
                data[res][key] = '0.0'

    if len(data.keys()) > 0:
        for res,record in data.items():
            query = """ 
            INSERT INTO swiggy_discount_metrics (business_date, created_at, updated_at, restaurant_id, orders, revenue,
            spends, aov, new_user_orders, repeat_user_orders, dormant_user_orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            execute_query(query,tuple(record.values()))

        connection.commit()

        printLog("--data scraped.")
        driver.switch_to.default_content()

        return True
    
    else:
        printLog("--No data available")
        return True

def process_discount_performance(driver,restaurant_ids,restaurants_names,phone_):
    """
    Scrapes discount performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
        phone_ (str): Phone number associated with the account.
    """
    
    def retry():
        url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
        driver.get(url)
        
        refresh(driver)

    cities_ = list({res.split(',')[-1].strip() for res in restaurants_names})

    url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
    driver.get(url)
    
    refresh(driver)

    query = """
        SELECT DISTINCT date
        FROM swiggy_discount_month_track
        WHERE restaurant_id IN %(restaurant_list)s;
        """
    cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})

    record = cur.fetchone()                   

    if record is None:
        printLog("--Record not available. Processing 30 days data.")
        
        for restaurant_id in restaurant_ids:

            query = """
                INSERT INTO swiggy_discount_month_track(
                restaurant_id,
                date
                ) 
                VALUES(
                    %s, %s
                    );
                """
            values = (restaurant_id,datetime.today().strftime('%Y-%m-%d'))
            execute_query(query,values)
            connection.commit()

        for i in range(len(cities_)):
            printLog(f"--Scraping for city: {cities_[i]}")

            try:
                wait_click(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[3]/button[1]') # compare button
            except Exception as e:
                printError(e)
                printLog("Path is changed or Webpage structure is changed. Please check the code.")
                return
            city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[2]')
            
            if city_pane is None:
                city_pane = driver.find_element(By.CLASS_NAME, 'fJoqsd')

            cities = city_pane.find_elements(By.CLASS_NAME, 'crpuGW')

            city = cities[i]
            city.click()

            wait_click(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button') #continue

            outlets = driver.find_elements(By.CLASS_NAME, 'crpuGW')
            for outlet in outlets:
                outlet.click()

            wait_click(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button') # continue

            driver.switch_to.default_content()
    
            for i in range(31,1,-1):
                date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
                check = select_date(driver,date)
                if not check:
                    driver.switch_to.default_content()
                    continue
                
                get_data_rev(driver,date)
                driver.switch_to.default_content()

                for restaurant_id in restaurant_ids:
                    query = """
                        UPDATE swiggy_discount_month_track
                        SET 
                            date = %s
                        WHERE 
                            restaurant_id = %s
                        """
                    values = (date, restaurant_id)
                    execute_query(query, values)

                connection.commit()
    
            refresh(driver)
            wait_click(driver,'//*[text()="Go To Discount Dashboard Home"]')
     
    else:
        printLog("--Record available. Downloading new data...")
        ids_str = ','.join(map(str, restaurant_ids))
        cur.execute(f"select distinct business_date from swiggy_discount_metrics where restaurant_id in ({ids_str})")
        records = cur.fetchall()
        scraped_dates = [str(record[0]) for record in records]

        previous_date = record[0]
        current_date = datetime.today().date()
        delta_days = (current_date - previous_date).days
        if delta_days > 30:
            printLog("--days to scraped are more than 30. Please run process ads file.")
            return
        
        for i in range(len(cities_)):
            printLog(f"--Scraping for city: {cities_[i]}")

            compare_button_xpath = '//button[text()="Compare Performance Of Outlets"]'
            compare_button = wait_visible(driver,compare_button_xpath,10)
            if compare_button is None:
                retry()
            
            try:
                wait_click(driver,compare_button_xpath)
                city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[2]')
                if city_pane is None:
                    city_pane = driver.find_element(By.CLASS_NAME, 'fJoqsd')
                cities = city_pane.find_elements(By.CLASS_NAME, 'crpuGW')
                
                for city in cities:
                    if city.text == cities_[i]:
                        city.click()
                        break

                wait_click(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button') #continue

                outlets = driver.find_elements(By.CLASS_NAME, 'crpuGW')
                for outlet in outlets:
                    outlet.click()

                wait_click(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button') # continue
                single_account = False

            except Exception as e:
                printLog("ALERT!!! Account is having single restaurant. Scraping data.")
                single_account = True
                    
            driver.switch_to.default_content()

            for i in range(delta_days, 0, -1):
                delta = current_date - timedelta(days=i)
                date = delta.strftime('%Y-%m-%d')

                if date in scraped_dates:
                    printLog("Date is already scraped. Skipping date.")
                    continue
                check = select_date(driver,date)
                
                if not check:
                    check = select_date(driver,date)
                    with open('dis_dates_with_error.txt','+a') as file:
                        file.write(date)
                        file.write('\n')
                    
                if single_account:
                    check = get_rev_single_res(driver,restaurant_ids[0],date)

                    if not check:
                        get_rev_single_res(driver,restaurant_ids[0],date)

                else:
                    check = get_data_rev(driver,date)

                    if not check:
                        get_data_rev(driver,date)
                    
                driver.switch_to.default_content()

            for restaurant_id in restaurant_ids:
                query = """
                    UPDATE swiggy_discount_month_track
                    SET 
                        date = %s
                    WHERE 
                        restaurant_id = %s
                    """
                values = (date, restaurant_id)
                execute_query(query, values)
            connection.commit()

            refresh(driver)

            if not single_account:
                try:
                    wait_click(driver,'//*[text()="Go To Discount Dashboard Home"]')
                except:
                    driver.refresh()
    
### ************ MAIN CODE HANDLING ALL STUFF ************ ###
def process_account(phone_,pass_):
    """
    Handles the process for each account, including logging in and calling subsequent scraping functions.

    Args:
        phone_ (str): Phone number of the account.
        pass_ (str): Password of the account.
    """

    cookies = get_cookies(phone_)
    driver = init_driver(cookies,False)
    
    driver.refresh()
    driver.maximize_window()

    time.sleep(4)

    printLog("\n#------ Scraping account: {} ------#".format(phone_))

    if driver.current_url == 'https://partner.swiggy.com/login/':
        printLog("--Cookies expired. Creating new session.")
        
        phone_placeholer = wait_visible(driver,'//input[@aria-label="Enter Restaurant ID / Mobile number"]')
        phone_placeholer.send_keys(phone_)

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="submit-phone-number"]')

        time.sleep(3)
        wait_click(driver,'//*[text()="Login with Password"]')

        password_placeholder = wait_visible(driver,'//input[@type="password"]')
        password_placeholder.send_keys(pass_)

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="Login-Button"]')

        create_cookies(driver,phone_)

    driver.get('https://partner.swiggy.com/orders')
    # https://partner.swiggy.com/login?next=orders
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source,features='lxml')

    main_divs = soup.find_all('div',class_='outlet-item-inner')

    restaurant_ids = [div.find('div',string=re.compile(r'\d+')).text for div in main_divs]

    clean_text = lambda x : re.sub(r'\s+',' ',x).strip()
    restaurants_names = [clean_text(div.find('div',class_='rest-details').text) for div in main_divs]

    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    if len(restaurant_ids) < 1:
        printLog("--Something went wrong. Please check code.")
        driver.quit()
        return

    ### ************ Processing Ad Performance ************ ###
    printLog("\n# ****** Scraping Ad Performance for all outlets. ****** #\n")
    process_ad_performance(driver,restaurant_ids)

    # ### ************ Processing Discount Performance ************ ###
    printLog("\n# ****** Scraping Discount Performance for all outlets. ****** #\n")
    process_discount_performance(driver,restaurant_ids,restaurants_names,phone_)

    driver.quit()

def clearFolders():
    """
    Clears and recreates the required directories for downloads and data.
    """
    if os.path.exists('downloads'):
        shutil.rmtree('downloads')
    os.makedirs('downloads', exist_ok=True)

    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data', exist_ok=True)

def main():
    """
    Main function to read account details and process each account.
    """

    df = pd.read_excel("accounts.xlsx")

    for i in range(len(df)):
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password

        clearFolders()
        
        process_account(phone_,pass_)
    
    printLog("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()
