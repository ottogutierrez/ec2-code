import boto3
import pandas as pd
from io import BytesIO
import requests
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm


class Product:
    def __init__(self):
        self.kwr_category = "SATA Erasers"
        self.rank = None
        self.part_number = None
        self.title = None
        self.image_url = None
        self.url = None
        self.original_price = None
        self.advertised_price = None
        self.is_discounted = None
        self.in_stock = None
        self.star_rating = None
        self.rating_count = None
        self.availability = None
        self.category_tree = None
        self.brand = None
        self.phrase = None


def scrape(search_term, results_list):
    url = "https://www.cdw.com/search/?key=" + search_term.replace(" ", "%20")
    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find("div", class_='search-results')
    global phrases_not_found
    # Scrape results
    try:
        foundProducts = results.find_all("div", class_="search-result")
        for tempProduct in foundProducts:
            p_object = Product()
            p_object.phrase = search_term
            p_object.part_number = tempProduct.find(
                "span", class_='mfg-code').text.split(" ")[1]
            p_object.title = tempProduct.find("h2").text.replace('\n', '')
            p_object.url = 'https://cdw.com' + \
                tempProduct.find('a', href=True)['href']
            p_object.image_url = tempProduct.find('img')['src']
            p_object.rank = tempProduct.find('a', href=True)['data-sort-rank']
            p_object.in_stock = True if tempProduct.find(
                "span", class_="-in-stock") else False
            try:
                p_object.star_rating = tempProduct.find(
                    "div", class_="star-rating-container")['data-rating']
                p_object.rating_count = tempProduct.find(
                    "div", class_="star-rating-count").text.strip("\r\n () \r\n")
            except:
                p_object.star_rating = 0
                p_object.rating_count = 0
            try:
                p_object.advertised_price = tempProduct.find(
                    "div", class_="price-type-price").text
                p_object.is_discounted = True
            except:
                p_object.advertised_price = tempProduct.find(
                    "div", class_="request-price-message")
            try:
                p_object.original_price = tempProduct.find(
                    "div", class_="price-msrp single").text
            except:
                p_object.original_price = '$0'
                p_object.is_discounted = False
            specs_page = requests.get(p_object.url)
            temp_specs = BeautifulSoup(specs_page.content, 'html.parser')
            try:
                p_object.availability = temp_specs.find(
                    "span", class_="message availability in-stock").text
            except:
                p_object.availability = temp_specs.find(
                    "span", class_="message availability").text
            tree_cats = temp_specs.find(
                "ul", class_="breadcrumbs").findAll("li")
            cat_string = ""
            for item in tree_cats:
                cat_string = cat_string + "/" + item.find("a").text
            p_object.category_tree = cat_string
            # get the brand name
            p_object.brand = temp_specs.find(
                "div", {"itemprop": "brand"}).find("meta")['content']
            results_list.append(p_object)
            # print(p_object.url)
            sleep(1)
    except:
        print('no search results for ' + search_term)
        phrases_not_found.append(search_term)
        pass


def scrape():
    # Get the uploaded file's details
    bucket = 'scrape-input'
    key = 'input.xlsx'

    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Read the uploaded Excel file into a DataFrame
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    df = pd.read_excel(BytesIO(data), engine='openpyxl')

    # Modify the DataFrame
    df = df[['phrase', 'volume_score', 'rank_score',
             'frequency_score', 'total_score', 'KW Classification2']]
    df = df[df['KW Classification2'] != "Don't use?"]
    search_results = []
    phrases_not_found = []
    n = df.shape[0]  # total number of iterations
    progress = 0  # progress counter

    for index, row in df.iterrows():
        temp_phrase = row['phrase']
        scrape(search_term=temp_phrase, results_list=search_results)
     # update the progress counter
    progress += 1
    # show the progress bar
    progress_bar = tqdm(total=n, desc='Processing', position=0, leave=True)
    progress_bar.update(progress)
    # Check how many phrases were not found
    if len(phrases_not_found) > 0:
        print("Need to run Selenium for " +
              str(len(phrases_not_found)) + " phrases")

    search_results_dicts = [product.__dict__ for product in search_results]
    df2 = pd.DataFrame(search_results_dicts)
    df2 = pd.DataFrame.merge(df2, df, on='phrase',
                             how='outer', suffixes=(None, None))

    print("Done scraping")
    # Save the modified DataFrame to a new Excel file in the specified S3 bucket
    output_bucket = 'scrape-output2'  # Replace with your desired output bucket name
    output_key = 'modified-' + key

    with BytesIO() as buffer:
        df2.to_excel(buffer, index=False, engine='openpyxl')
        buffer.seek(0)
        s3.put_object(Bucket=output_bucket, Key=output_key,
                      Body=buffer.getvalue())


if __name__ == "__main__":
    scrape()
