from modules import *
import os
import pandas as pd
import logging
from datetime import datetime
import sys

cur,connection = get_cursor()

current_date = datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(filename = f"logs/swiggy_ad_date_error_logs_{current_date}.log", 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

def printLog(msg,end=None):
    logging.info(msg)
    if end is None:
        print(msg)
    else:
        print(msg,end=end)

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
    try:
        cur.execute(query, values)
    except Exception as e:
        printError(e,True)
        connection.commit()

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
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.XPATH,'//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source,features='lxml')
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

    wait_click(driver,'//*[text()="New Users"]',5)

    data_available = wait(0)

    if not data_available:
        printLog("Data not fetched from 1st pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source,features='lxml')

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
        driver.refresh()
        try:
            wait_click(driver,'/html/body/div[2]/div/div/button')
        except:
            pass
        return True

    data_available = wait(1)

    if not data_available:
        printLog("Data not fetched from 2nd pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source,features='lxml')

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
            
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['nu_orders'] = '0.0'
    

    driver.switch_to.default_content()
    driver.switch_to.frame(iframe)

    wait_click(driver,'//*[text()="Slot Types"]')

    data_available = wait(1)

    soup = BeautifulSoup(driver.page_source,features='lxml')

    cells = soup.find_all('div', class_='pivotTableCellWrap')
    
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

    except:
        
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
   
def process_ad_performance(driver,restaurant_ids,custom_date=None):

    driver.get(f'https://partner.swiggy.com/business-metrics/ads-performance/restaurant/{restaurant_ids[0]}')
    
    time.sleep(3)
    
    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    check = select_date(driver,custom_date)

    if not check:
        driver.switch_to.default_content()
        return
    
    try:
        wait_click(driver,'//*[text()="Outlet level Performance"]',2)
    except:
        pass
    
    get_data(driver,custom_date)

### ************ DISCOUNT PERFORMANCE ************ ###
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

            soup = BeautifulSoup(driver.page_source,features='lxml')
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
    
    soup = BeautifulSoup(driver.page_source,features='lxml')
    
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

    soup = BeautifulSoup(driver.page_source,features='lxml')

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

def process_discount_performance(driver,restaurant_ids,restaurants_names,phone_,date):

    def retry():
        url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
        driver.get(url)
        
        refresh(driver)

    cities_ = list({res.split(',')[-1].strip() for res in restaurants_names})
    
    ids_str = ','.join(map(str, restaurant_ids))
    cur.execute(f"select distinct business_date from swiggy_discount_metrics where restaurant_id in ({ids_str})")
    records = cur.fetchall()
    scraped_dates = [str(record[0]) for record in records]
    
    if date in scraped_dates:
        return
    
    url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
    driver.get(url)
    
    refresh(driver)

        
    # date = datetime.strptime(date,'%Y-%m-%d')
    for i in range(len(cities_)):
        printLog(f"--Scraping for city: {cities_[i]}")

        compare_button_xpath = '//button[text()="Compare Performance Of Outlets"]'
        compare_button = wait_visible(driver,compare_button_xpath,10)
        if compare_button is None:
            retry()
        
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

        driver.switch_to.default_content()

        check = select_date(driver,date)
        if not check:
            driver.switch_to.default_content()
            continue
                    
        get_data_rev(driver,date)
        
        refresh(driver)

        try:
            wait_click(driver,'//*[text()="Go To Discount Dashboard Home"]')
        except:
            driver.refresh()
    
    query = """
            INSERT INTO swiggy_discount_month_track(
            account,
            date
            ) 
            VALUES(
                %s, %s
                );
            """
    values = (phone_,datetime.today().strftime('%Y-%m-%d'))
    execute_query(query,values)
    connection.commit()

def process_account(phone_,pass_):
    cookies = get_cookies(phone_)
    
    driver = init_driver(cookies,False)
    
    driver.refresh()
    driver.maximize_window()

    time.sleep(4)

    print("\n#------ Scraping account: {} ------#".format(phone_))

    if driver.current_url == 'https://partner.swiggy.com/login/':
        print("--Cookies expired. Creating new session.")
        
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
        print("--Something went wrong. Please check code.")
        driver.quit()
        return
    
    printLog("Processing custom dates for ad metrics")
    with open('ad_dates_with_error.txt','r') as file:
        dates_with_error = file.read().split('\n')
    
    for date in dates_with_error:
        if date == '':
            continue
        process_ad_performance(driver,restaurant_ids,date)

    printLog("Processing custom dates for discount metrics")
    
    with open('dis_dates_with_error.txt','r') as file:
        dates_with_error = file.read().split('\n')
    
    for date in dates_with_error:
        if date == '':
            continue
        process_discount_performance(driver,restaurant_ids,restaurants_names,phone_,date)

    with open('ad_dates_with_error.txt', 'w') as file:
        file.write('')

    with open('dis_dates_with_error.txt', 'w') as file:
        file.write('')

    driver.quit()

def main():
    df = pd.read_excel("new_accounts.xlsx")

    for i in range(len(df)):
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password
        
        process_account(phone_,pass_)
    
    with pd.ExcelWriter("new_accounts.xlsx", mode='w') as writer:
        data = [{'Phone' : '',
        'Password': ''}]
        empty_df = pd.DataFrame(data)
        empty_df.to_excel(writer, index=False)
    printLog("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()
