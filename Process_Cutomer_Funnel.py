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

### ************ CUSTOMER FUNNEL ************ ###

def handle_container_popups(driver):
    """
    Handles potential popups in the #mCSB_1_container before proceeding.
    """
    try:
        # Wait for the container to load
        container_xpath = '//div[@id="mCSB_1_container"]'
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, container_xpath))
        )

        # Look for popup close buttons inside the container
        popup_close_button_xpath = '//a[contains(@class, "dismiss") or contains(@class, "icon-close")]'  # Adjust as needed
        popups = driver.find_elements(By.XPATH, popup_close_button_xpath)
        
        if popups:
            for popup in popups:
                try:
                    wait_click(driver,popup_close_button_xpath)
                    time.sleep(1)  # Allow time for the popup to dismiss
                    print("Popup closed successfully.")
                except Exception as e:
                    print(f"Could not close popup: {e}")
        else:
            print("No popups found in the container.")
    except Exception as e:
        print(f"Error handling popups: {e}")

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

def select_date_funnel(driver, date):
    driver.switch_to.default_content()

    try:
        wait_click(driver, '/html/body/div[2]/div/div/button')  # Popup close button if it appears
    except:
        pass

    time.sleep(3)

    # 🔁 Switch to correct iframe
    printLog("Switching to iframe")
    driver.switch_to.frame("mfe-frame")

    # ✅ Click "Today" button
    today_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div/div[1]'
    printLog("Clicking Today button")
    wait_click(driver, today_button_xpath)
    time.sleep(1)

    # ✅ Click "Custom" button
    custom_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div[7]/div[1]'
    printLog("Clicking Custom button")
    try:
        wait_click(driver, custom_button_xpath)
        time.sleep(2)
    except:
        driver.switch_to.default_content()
        popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[4]/ul/div/div/div[1]/a'
        try:
        # Check if the popup close button exists and is interactable
            close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
            if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                wait_click(driver, popup_close_xpath)  # Click if interactable
            else:
                print("Popup exists but is not interactable or does not need closing")
        except:
            print(f"error handling popup 1")

        driver.switch_to.frame("mfe-frame")
        wait_click(driver, custom_button_xpath)
        time.sleep(2)

        

    # 📅 Setup date
    provided_date = datetime.strptime(date, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    try:
        printLog("Locating input fields")
        from_input_element = wait_visible(driver, '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[2]/div[1]/div[1]')
        to_input_element = wait_visible(driver, '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[2]/div[2]/div[1]')
    except Exception as e:
        printLog(f"Error locating input fields: {e}")
        return False

    try:
        # Locate the current visible calendar month text (add xpath)
        calendar_month_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[3]/div/div[1]/button[3]/span'
        month_element = wait_visible(driver, calendar_month_xpath)
        if not month_element:
            printLog("Could not find calendar month element")
            return False

        current_month_text = month_element.text  # Example: 'March 2025'
        current_date = datetime.strptime(current_month_text, '%B %Y')

        while current_date.month != provided_month or current_date.year != provided_year:
            if provided_date < current_date:
                printLog("Navigating to previous month")
                prev_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[3]/div/div[1]/button[2]'
                wait_click(driver, prev_button_xpath)
            else:
                printLog("Navigating to next month")
                next_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[3]/div/div[1]/button[4]'
                wait_click(driver, next_button_xpath)
            time.sleep(1.5)

            month_element = wait_visible(driver, calendar_month_xpath)
            current_month_text = month_element.text
            current_date = datetime.strptime(current_month_text, '%B %Y')

        printLog(f"Searching for date button with value: {day}")
        date_buttons = driver.find_elements(By.CLASS_NAME, "react-calendar__tile")
        found = False
        
        expected_full_date = provided_date.strftime('%B %#d, %Y')  # Use '%#d' for Windows

        for button in date_buttons:
            try:
                abbr = button.find_element(By.TAG_NAME, "abbr")
                if abbr:
                    full_date_text = abbr.get_attribute('aria-label') or abbr.get_attribute('title')
                    if full_date_text == expected_full_date:
                        printLog(f"Clicking date button for {expected_full_date}")
                        button.click()
                        time.sleep(0.3)
                        button.click()
                        found = True
                        break
            except Exception as e:
                printLog(f"Error while checking date button: {e}")
                continue

        if not found:
            printLog("Could not find the date in the calendar")
            return False

        # Check if from and to values match
        start = from_input_element.text
        print(start)
        end = to_input_element.text
        print(end)

        if are_dates_same(start, end):
            printLog(f"Date selected: {date}")
            confirm_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[4]/div'
            wait_click(driver, confirm_button_xpath)
            return True
        else:
            printLog(f"Date selection mismatch - From: {start}, To: {end}")
            Close_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[1]/div[2]/svg/circle'
            wait_click(driver,Close_button_xpath)
            return False

    except Exception as e:
        printLog(f"Exception during date selection: {e}")
        Close_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[1]/div[2]/div/div[1]/div[2]/svg/circle'
        wait_click(driver,Close_button_xpath)
        return False

def scrape_customer_funnel(driver, restaurant_id, date):
    """
    Scrapes customer funnel data (Menu, Cart, Orders) for a given restaurant and date,
    and stores it into the Swiggy_customer_funnel table.
    """
    try:
        print(f"[INFO] Starting funnel scrape for Restaurant ID: {restaurant_id}, Date: {date}")

        # Step 1: Switch to correct iframe
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame("mfe-frame")
            print("[INFO] Switched to 'mfe-frame'")
        except Exception as e:
            print(f"[ERROR] Failed to switch to iframe: {e}")
            return

        # Step 2: Define XPaths
        Menu_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div/div[2]/div/div/div[2]/div/div/div[1]/div[1]/div[2]'
        try:
            wait_visible(driver,Menu_xpath)
        except:
            print(f"Could not find menu")
        Cart_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div/div[2]/div/div/div[2]/div/div/div[2]/div[1]/div[2]'
        try:
            wait_visible(driver,Cart_xpath)
        except:
            print(f"Could not find cart")
        Orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div/div[2]/div/div/div[2]/div/div/div[3]/div/div[2]'
        try:
            wait_visible(driver,Orders_xpath)
        except:
            print(f"Could not find orders")

        # Step 3: Extract Menu data
        try:
            menu_raw = driver.find_element(By.XPATH, Menu_xpath).text
            menu = int(menu_raw.replace(',', ''))
            print(f"[INFO] Menu count: {menu}")
        except Exception as e:
            print(f"[ERROR] Failed to get menu data: {e}")
            return

        # Step 4: Extract Cart data
        try:
            cart_raw = driver.find_element(By.XPATH, Cart_xpath).text
            cart = int(cart_raw.replace(',', ''))
            print(f"[INFO] Cart count: {cart}")
        except Exception as e:
            print(f"[ERROR] Failed to get cart data: {e}")
            return

        # Step 5: Extract Orders data
        try:
            orders_raw = driver.find_element(By.XPATH, Orders_xpath).text
            orders = int(orders_raw.replace(',', ''))
            print(f"[INFO] Orders count: {orders}")
        except Exception as e:
            print(f"[ERROR] Failed to get orders data: {e}")
            return

        # Step 6: Insert into database
        try:
            query = """
                INSERT INTO Swiggy_customer_funnel (
                    restaurant_id, business_date, menu, cart, orders
                ) VALUES (%s, %s, %s, %s, %s);
            """
            values = (restaurant_id, date, menu, cart, orders)
            execute_query(query, values)
            connection.commit()
            print(f"[SUCCESS] Funnel data saved for {restaurant_id} on {date}")
        except Exception as e:
            print(f"[ERROR] Database insertion failed: {e}")
            return

    except Exception as e:
        print(f"[FATAL ERROR] Unexpected error in scrape_customer_funnel: {e}")

def process_customer_funnel(driver,restaurant_ids):
    
    """
    Scrapes discount performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
        phone_ (str): Phone number associated with the account.
    """
    
    def retry():
        url = f'https://partner.swiggy.com/business-metrics/overview/restaurant/{restaurant_ids[0]}'
        driver.get(url)        
        refresh(driver)
        handle_container_popups(driver)
        popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
        try:
        # Check if the popup close button exists and is interactable
            close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
            if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                wait_click(driver, popup_close_xpath)  # Click if interactable
            else:
                print("Popup exists but is not interactable or does not need closing")
        except:
            print(f"error handling popup 1")

    for restaurant_id in restaurant_ids:
        retry()

        driver.switch_to.frame("mfe-frame")

        Outlet_filter_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[7]/div'
        wait_click(driver,Outlet_filter_button_xpath)
        
        Select_all_checkbox_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[8]/div/div[2]/div/div[1]/div/div[2]'
        wait_visible(driver,Select_all_checkbox_xpath)
        wait_click(driver,Select_all_checkbox_xpath) # deselects all outlets

        scroll_container_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[8]/div/div[2]/div/div[1]'

        container = driver.find_element(By.XPATH, scroll_container_xpath)
        prev_scroll = -1

        while True:
            elements = driver.find_elements(By.CLASS_NAME, "styled__OptionContainer-business-metrics-mfe__sc-as1n8o-6")
            for el in elements:
                try:
                    rid_element = el.find_element(By.XPATH, ".//div[contains(text(), 'RID:')]")
                    rid_text = rid_element.text.strip()
                    if rid_text == f"RID: {restaurant_id}":
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", el)
                        time.sleep(0.5)
                        el.click()
                        print(f"[INFO] Clicked on RID: {restaurant_id}")
                        break
                except:
                    continue
            else:
                # Scroll further down
                driver.execute_script("arguments[0].scrollBy(0, 200);", container)
                time.sleep(0.5)
                new_scroll = driver.execute_script("return arguments[0].scrollTop", container)
                if new_scroll == prev_scroll:
                    print(f"[WARN] RID {restaurant_id} not found.")
                    break
                prev_scroll = new_scroll
                continue
            break

        Apply_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[1]/div[8]/div/div[2]/div/div[2]/div/button'
        wait_click(driver,Apply_button_xpath)

        query = """
                SELECT DISTINCT date
                FROM Swiggy_customer_funnel_month_track
                WHERE restaurant_id = %s;
                """
        cur.execute(query, (int(restaurant_id),))
        record = cur.fetchone()
        
        if record is None: 
            printLog("--Record not available. Processing 180 days data.")           

            for i in range(180,1,-1):
                date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')

                check = select_date_funnel(driver,date)

                if not check:
                    driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                    continue
                            
                scrape_customer_funnel(driver,restaurant_id,date)
                               
                driver.switch_to.default_content()
                
            print(f"data scraped for {restaurant_id}")            
            query = """
                    INSERT INTO Swiggy_customer_funnel_month_track(
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


            print(f"Swiggy_months_track updated for {restaurant_id}")

        else:
            printLog("--Record available. Downloading new data...")

            query = """
                SELECT DISTINCT business_date
                FROM Swiggy_customer_funnel
                WHERE restaurant_id = %s
                ORDER BY business_date DESC;
            """
            cur.execute(query, (restaurant_id,))
            records = cur.fetchall()
            scraped_dates = [str(record[0]) for record in records]
            previous_date = record[0]
            print(f"previous_date:({previous_date})")
            current_date = datetime.today().date()
            delta_days = (current_date - previous_date).days

            for i in range(delta_days, 0, -1):
                delta = current_date - timedelta(days=i)
                date = delta.strftime('%Y-%m-%d')

                if date in scraped_dates:
                    printLog("Date is already scraped. Skipping date.")
                    continue

                check = select_date_funnel(driver,date)

                scrape_customer_funnel(driver,restaurant_id,date)
                
                query = """
                    UPDATE Swiggy_customer_funnel_month_track
                    SET 
                        date = %s
                    WHERE 
                        restaurant_id = %s
                    """
                values = (date, restaurant_id)
                execute_query(query, values)
                connection.commit()                   
                        

            print(f"data scraped for {restaurant_id}")
            print(f"Swiggy_months_track updated for {restaurant_id}")

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
    
    # ### ************ Processing Discount Performance ************ ###
    printLog("\n# ****** Scraping Customer Funnel for all outlets. ****** #\n")
    process_customer_funnel(driver,restaurant_ids)

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