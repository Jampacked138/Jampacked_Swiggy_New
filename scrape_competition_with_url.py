from selenium import webdriver
import time
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import os
import pandas as pd
from modules import *
import re
import json
import logging

logging.basicConfig(filename = "logs_without_login.log", 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )
cur,connection = get_cursor()

# proxy = get_proxy()

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

def execute_query(query, values):
    try:
        cur.execute(query, values)
        return True
    except Exception as e:
        printError(e,True)
        connection.commit()
        return False

    
def extract_cost_and_people(content_):
    # Extract the cost
    cost_match = re.search(r'₹([\d,]+)', content_)
    cost_str = cost_match.group(1) if cost_match else None

    if cost_str:
        # Remove commas and get the integer value of the cost
        cost = int(cost_str.replace(',', ''))
    else:
        cost = None

    # Extract the "for one" or "for two" part
    people_match = re.search(r'for (one|two)', content_)
    people = people_match.group(1) if people_match else None

    return cost, people

def get_dishes(soup):
    costs = [cost.text.strip() for cost in soup.find_all('div', class_="sc-aXZVg iGZTFL")]
    dishes = [dish.text.strip() for dish in soup.find_all('div', class_="sc-aXZVg kDrzID sc-eldPxv gKfzLy")]
    has_pictures = [False if soup.find('img',attrs={"alt" : dish}) is None else True for dish in dishes]

    elements = soup.find_all('div', class_="sc-aXZVg iGZTFL")
    ratings = []

    for element in elements:
        find = element.find_parent('div',class_="sc-ikkxIA iDlNXU").next_sibling
        if find is not None:
            searches = re.search(r'(\d+.\d+)\S(\d+)\S',find.text)
            if searches is None:
                ratings.append((0.0,0))
            else:
                rating = searches.group(1)
                count = searches.group(2)
                ratings.append((rating,count))
        else:
            ratings.append((0.0,0))    

    has_descriptions = []

    for element in elements:
        find = element.find_parent('div',class_="sc-ikkxIA iDlNXU").parent.select_one('div.sc-aXZVg.fSrSXg')
        has_description = False if find is None else True
        has_descriptions.append(has_description)

    categories = []

    elements = soup.find_all('div', class_="sc-aXZVg kDrzID sc-eldPxv gKfzLy")

    for element in elements:
        try:
            sub_element = element.find_parent('div',attrs={"data-testid":"normal-dish-item"}).parent.parent.parent
            category = re.search( r"[\w\s&]+",sub_element.find('h3').text).group(0)
        except:
            category = "Not Available"

        categories.append(category.strip())

    data = []

    for i in range(len(dishes)):
        data_point = {
            "dish_name" : dishes[i],
            "price" : costs[i],
            "rating" : ratings[i][0],
            "votes" : ratings[i][1],
            "has_pictures" : has_pictures[i],
            "category" : categories[i],
            "has_description" : has_descriptions[i],
        }

        data.append(data_point)
    
    return data

def parse_ratings(ratings_str):
    if 'K' in ratings_str:
        num_str = ratings_str.replace('K', '000')
    else:
        num_str = ratings_str
    
    num_str = ''.join(c for c in num_str if c.isdigit())

    return int(num_str)

def scrape_restaurant(driver, restaurant_id):
    date_ = get_current_date_formated()
    soup = BeautifulSoup(driver.page_source,features='lxml')

    restaurant_name = wait_visible(driver, '//*[@id="root"]/div[1]/div/div/div/div[2]/div[2]/div/h1').text
    area_name = driver.find_element(By.CSS_SELECTOR, 'div.sc-aXZVg.fVWuLc.sc-isRoRg.kgrKzS').text
    try:
        div = soup.find('div', class_='sc-empnci cFowAQ')
        children = div.findChildren('div', recursive=False)
        for child in children:
            content = child.text
            if 'ratings' in content:
                rating = content.split(' ')[0]
            elif 'for' in content:
                cost, people = extract_cost_and_people(content)
    except:
        rating = 0.0
        cost = 0

    delivery_time = ''
    try:
        div = soup.find('div', class_='sc-eoVZPG gPxPwq')
        children = div.findChildren('div', recursive=False)
        for child in children:
            content = child.text
            if 'mins' in content:
                delivery_time = content
    except:
        delivery_time = 'Not available'

    dish_details = get_dishes(soup)

    promos = []
    for promo in soup.find_all('div',class_ = "sc-dZoequ diYSKy"):
        divs = promo.find_all('div')
        promo = {
            "promo" : divs[0].text,
            "promo_instruction" : divs[1].text
        }
        promos.append(promo)

    check = soup.find('p',class_="RestaurantLicence_licenceText__2XEQc")
    fssai = 'NA' if check is None else re.search(r'\d+',check.text).group(0)

    check = soup.find('div',string=re.compile(r'(₹)\d+\s(for)\s\S+'))
    cost_for_two = '0' if check is None else re.search(r'\d+',check.text).group(0)

    check = soup.find('div',class_="sc-aXZVg kauPDe",string=re.compile(r'.+\s(ratings?)'))
    pattern = re.compile(r'\((.*?)\)')
    raw_text = soup.find('div',class_="sc-aXZVg kauPDe",string=pattern)
    rating_str = '0' if check is None else re.search(pattern,check.text).group(1)
    delivery_votes_bucket = parse_ratings(rating_str)

    scraped_data_restaurant = {
        'created_at': date_,
        'updated_at': date_,
        'restaurant_id': restaurant_id,
        'restaurant_name': restaurant_name,
        'area_name': area_name,
        'rating': rating,
        'cost': cost,
        'cost_for_people': people,
        'delivery_time': delivery_time,
        'promos': json.dumps(promos, indent=1),
        'dish_details': json.dumps(dish_details, indent=1),
        'fssai': fssai,
        'delivery_votes_bucket': delivery_votes_bucket
    }

    query = """
            INSERT INTO swiggy_competition_from_url (
                created_at, updated_at, restaurant_id, restaurant_name, area_name, rating, 
                cost, cost_for_people, delivery_time, promos, dish_details, 
                fssai, delivery_votes_bucket
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s,
                %s, %s
            );
        """
    values = tuple(scraped_data_restaurant.values())
    execute_query(query, values)
    connection.commit()


def get_urls_from_excel(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Assume the column containing URLs is named 'url'
    urls = df['url'].tolist()
    restaurant_id = df['restaurant_id'].tolist()
    tuple_list = list(zip(urls, restaurant_id))


    return tuple_list




def main():

    file_path = 'scrape_competition.xlsx'
    tuple_list = get_urls_from_excel(file_path)

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-notifications")
    proxy_address = '192.53.137.222'
    proxy_username = 'fpcnshwd'
    proxy_port = '6510'
    proxy_password = '47ka06elhlhf'
    
    proxies_extension = proxies(proxy_username, proxy_password, proxy_address, proxy_port)
    chrome_options.add_extension(proxies_extension)

    driver = webdriver.Chrome(options=chrome_options)
    driver.get('https://www.swiggy.com/city/delhi')
    driver.maximize_window()

    # url = 'https://www.swiggy.com/restaurants/subway-sector-22-chandigarh-42803'
    # url = 'https://www.swiggy.com/restaurants/uttam-sweets-bakery-and-restaurant-sector-34-chandigarh-555174'
    # url = 'https://www.swiggy.com/restaurants/karims-delhi-59-hastsal-uttam-nagar-delhi-251831'
    # url = 'https://www.swiggy.com/restaurants/sardar-ji-late-night-kitchen-a-block-paschim-vihar-delhi-504341'
    # url = 'https://www.swiggy.com/restaurants/roll-nama-b-block-rajouri-garden-delhi-481900'
    # restaurant_id = 12345678
    # driver.get(url)
    # time.sleep(5)
    # scrape_restaurant(driver, restaurant_id)
    for url, restaurant_id in tuple_list:
        driver.get(url)
        driver.maximize_window()
        time.sleep(2)
        scrape_restaurant(driver, restaurant_id)
        time.sleep(2)

if __name__ == "__main__":
    main()