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

LIMIT = 10

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

def execute_query(query,values):
    try:
        cur.execute(query, values)
        return True
    except Exception as e:
        printError(e,True)
        connection.commit()
        return False

def parse_ratings(ratings_str):
    if 'K' in ratings_str:
        num_str = ratings_str.replace('K', '000')
    else:
        num_str = ratings_str
    
    num_str = ''.join(c for c in num_str if c.isdigit())

    return int(num_str)

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

def scrape_brands(soup,area_search_for,city):

    query = f"""SELECT area_search_for, city
    FROM swiggy_local_top_brands
    WHERE area_search_for = '{area_search_for}' AND
    city = '{city}' 
    """

    cur.execute(query)
    record_exists = cur.fetchone()

    if record_exists:
        return
    
    # divs = soup.find_all("div",class_="sc-cCzKKE gxwMLH") old code
    check = True if soup.find('h2',string="Best Food Outlets Near Me") else False
    if check:
        div = soup.find_all('div',class_='row')[1]

        divs = div.findChildren('div',recursive=False)
    else:
        return

    data = []

    for sequence,div in enumerate(divs,start=1):

        pattern = re.compile(r"\b\d{1,2}-\d{1,2} mins\b")
        sub_divs = div.find_all(class_="sc-beySbM iEzVWe")

        brand_name = div.find(class_="sc-beySbM lfjhyG").text
        cuisines = sub_divs[0].text.split(",")
        area_name = sub_divs[1].text

        rating = div.find(class_='sc-beySbM jdpFZn').text.split(" ")[0]

        try:
            text = div.find(class_='sc-beySbM jdpFZn').parent.text
            delivery_time = re.search(pattern,text).group(0)
        except:
            try:
                delivery_time = div.find(class_='sc-beySbM jdpFZn').parent.text.split("•")[1].strip()
            except:
                delivery_time = "Not Available"    

        data_point = {
            "area_search_for" : area_search_for,
            "city" : city,
            "created_at" : get_current_date_formated(),
            "updated_at" : get_current_date_formated(),
            "brand_name" : brand_name,
            "sequence_number" : sequence,
            "rating" : rating,
            "delivery_time" : delivery_time,
            "cuisines" : cuisines,
            "area_name" : area_name ,
        }

        data.append(data_point)
    
    query = """
                INSERT INTO swiggy_local_top_brands(
                area_search_for, city, created_at, updated_at, brand_name, sequence_number, rating, delivery_time, cuisines, area_name
                )
                VALUES
                (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
                ;"""


    for record in data:
        values = tuple(record.values())
        execute_query(query,values)

    connection.commit()

    print("--Scraped Brands")

def scrape_cuisines(soup,area_search_for,city):

    data = []

    query = f"""SELECT area_search_for, city
    FROM swiggy_local_top_dishes
    WHERE area_search_for = '{area_search_for}' AND
    city = '{city}' 
    """

    cur.execute(query)
    record_exists = cur.fetchone()

    if record_exists:
        return
    
    # divs = soup.find_all("div",class_="sc-cCzKKE czJEnE") old code

    div = soup.find('div',class_='row')
    anchors = div.find_all('a')

    for sequence,anchor in enumerate(anchors,start=1):
        text = anchor['aria-label'].split('for ')[-1].capitalize()
        
        data_point = {
            "area_search_for" : area_search_for,
            "city" : city,
            "created_at" : get_current_date_formated(),
            "updated_at" : get_current_date_formated(),
            "dish_or_cuisine" : text,
            "sequence_number" : sequence
        }
        
        data.append(data_point)
        
    query = """
                INSERT INTO swiggy_local_top_dishes(
                area_search_for, city, created_at, updated_at, dish_or_cuisine, sequence_number
                )
                VALUES
                (%s, %s, %s, %s, %s, %s)
                ;"""

    for record in data:
        values = tuple(record.values())
        execute_query(query,values)

    connection.commit()
    print("--Scraped Cuisines")
  
def scrape_restaurants(driver,area_search_for,city):
    
    def scrape_local_competition(driver,soup):

        main_div = soup.find('div',class_="sc-gLLvby jXGZuP")
        divs = main_div.find('div').find_next_siblings('div')

        restaurants = []
        
        for sequence,div in enumerate(divs,start=1):
            
            restaurant_name = div.find('div', class_="sc-beySbM lfjhyG").text
            description_ele = div.find(class_='sw-restaurant-card-descriptions-container')
            cuisines = description_ele.find_all('div')[0].text.split(',')
            area_name = description_ele.find_all('div')[1].text
            raw_text = div.find('div',class_="sw-restaurant-card-subtext-container").text
            
            if "•" in raw_text:
                rating, delivery_time = raw_text.split('•')
            else:
                rating, delivery_time = 0.0 , raw_text

            url = div.a['href']

            data_point = {
            "area_search_for" : area_search_for ,
            "created_at" : get_current_date_formated(),
            "updated_at" : get_current_date_formated(),
            "sequence_number" : sequence,
            "competition_name" : restaurant_name,
            "cost_for_two" : '0',
            "delivery_time" : delivery_time,
            "cuisines" : cuisines,
            "area_name" : area_name,
            "city" : city,
            "delivery_rating" : rating,
            "delivery_votes_bucket" : '0',
            "promos" : '',
            "dish_details" : '',
            "fssai" : '',
            "url" : url
            }
            
            restaurants.append(data_point)

        print("--Total restaurants to scrape: ",len(restaurants))
        
        for restaurant in restaurants:
            url = restaurant.pop('url')
            driver.get(url)
            soup = BeautifulSoup(driver.page_source,features='lxml')

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

            restaurant["promos"] = json.dumps(promos,indent=1)
            restaurant["dish_details"] = json.dumps(dish_details,indent=1)
            restaurant["fssai"] = fssai
            restaurant["cost_for_two"] = cost_for_two
            restaurant["delivery_votes_bucket"] = delivery_votes_bucket

        query = """
            INSERT INTO swiggy_local_competition (
                area_search_for, created_at, updated_at, sequence_number, competition_name,
                cost_for_two, delivery_time, cuisines, area_name, city,
                delivery_rating, delivery_votes_bucket, promos, dish_details, fssai
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """
        
        for restaurant in restaurants:
            values = tuple(restaurant.values())
            execute_query(query,values)

        connection.commit()

        print("--Data scraped for local competition.")
    
    wait_click(driver,'//*[@id="seo-core-layout"]/div[1]/div/div/div[2]')
    input_field = wait_visible(driver,'//*[@id="seo-core-layout"]/div[1]/div/div/div[2]/div[2]/div/div[1]/input')
    input_field.send_keys(area_search_for)

    time.sleep(2)

    wait_click(driver,'//*[@id="seo-core-layout"]/div[1]/div/div/div[2]/div[2]/div/div[2]/div[2]')

    time.sleep(4)
    
    clicks = 0
    while clicks < 50:
        wait_click(driver,'//*[text()="Show more"]')
        clicks += 1
        time.sleep(0.5)
        soup = BeautifulSoup(driver.page_source,features='lxml')
        main_div = soup.find('div',class_="sc-gLLvby jXGZuP")
        divs = main_div.find('div').find_next_siblings('div')
        if len(divs) >= LIMIT:
            break

    soup = BeautifulSoup(driver.page_source,features='lxml')

    scrape_brands(soup,area_search_for,city)
    
    scrape_cuisines(soup,area_search_for,city)

    scrape_local_competition(driver,soup)
    
    print("--Data Scraped successfully.")
    
def main():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-notifications")
    proxy_address = '192.53.137.222'
    proxy_username = 'fpcnshwd'
    proxy_port = '6510'
    proxy_password = '47ka06elhlhf'
    
    proxies_extension = proxies(proxy_username, proxy_password, proxy_address, proxy_port)
    chrome_options.add_extension(proxies_extension)

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()

    cur.execute("select * from swiggy_restaurant_metrics")
    records = cur.fetchall()
    records = {(record[4],record[5]) for record in records}


    for record in records:
        area_name,city = record
        print("Scraping data for area: {},city : {}".format(area_name,city))

        query =f"""SELECT *
                FROM swiggy_local_competition
                WHERE area_search_for = '{area_name}' AND city = '{city}'
                """
        cur.execute(query)
        record_exists = cur.fetchone()

        if record_exists:
            print("Record exists")

        else:
            print("No restaurant found for this area and city. Scraping...")

            url = f'https://www.swiggy.com/city/{city.strip().capitalize()}'
            driver.get(url)
            
            scrape_restaurants(driver,area_name,city)


if __name__ == "__main__":
    main()