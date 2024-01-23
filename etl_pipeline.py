import luigi
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
from tqdm import tqdm
from helper.db_connection import postgres_engine

class ExtractDatabase(luigi.Task):
    
    def requires(self):
        pass

    def run(self):
        engine = postgres_engine()

        db_data = pd.read_sql(sql = "SELECT * FROM dev_mall_customer",
                              con = engine)
        
        db_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/raw/db_raw_data.csv")
    
class ScrapeData(luigi.Task):
    
    def requires(self):
        pass

    def run(self):
        # list to store all extracted data
        full_data = []

        for page in tqdm(range(1, 11)):
            time.sleep(0.25)

            resp = requests.get(f"https://quotes.toscrape.com/page/{page}")

            soup = BeautifulSoup(resp.text, "html.parser")

            raw_data = soup.find_all("div", class_ = "quote")

            for data in raw_data:
                # extract quotes data
                quotes = data.find("span").text

                # extract author data
                author = data.find("small").text

                # get tags data
                tags = data.find_all("a", class_ = "tag")

                # create empty list for store all tag value
                get_tag = []

                # iterate tag in each quotes card
                for tag in tags:
                    # store the tag value
                    get_tag.append(tag.text)

                data = {
                    "quote": quotes,
                    "author": author,
                    "tags": get_tag
                }

                full_data.append(data)

        quote_data = pd.DataFrame(full_data)

        quote_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/raw/scrape_raw_data.csv")
    
class TransformDatabaseData(luigi.Task):
    
    def requires(self):
        return ExtractDatabase()
    
    def run(self):
        extracted_data = pd.read_csv(self.input().path)

        RENAME_VALUES = {
            "Male": "male",
            "Female": "female"
        }

        extracted_data["gender"] = extracted_data["gender"].map(RENAME_VALUES)

        extracted_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/transform/db_transform_data.csv")
    
class TransformScrapeData(luigi.Task):

    def requires(self):
        return ScrapeData()
    
    def run(self):
        scrape_data = pd.read_csv(self.input().path)

        scrape_data["tags"] = scrape_data["tags"].apply(lambda x: ', '.join(x.strip("[]").replace("'", "").split(', ')))

        scrape_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/transform/scrape_transform_data.csv")
    
class LoadData(luigi.Task):
    
    def requires(self):
        # because we need to store the previous task into one database
        # we initiate previous task using list
        return [TransformDatabaseData(), TransformScrapeData()]
    
    def run(self):
        engine = postgres_engine()

        # load the transformed data from TransformDatabaseData and TransformScrapeData

        # access data based on index in requires method
        transform_data_db = pd.read_csv(self.input()[0].path)
        transform_data_scrape = pd.read_csv(self.input()[1].path)

        transform_data_db.to_csv(self.output()[0].path)
        transform_data_scrape.to_csv(self.output()[1].path)

        # load the data into database
        transform_data_db.to_sql(name = "db_mall_customers",
                                 con = engine,
                                 if_exists = "replace", # replace will drop your table and generate new based on input data
                                 index = False)
        
        transform_data_scrape.to_sql(name = "scrape_table",
                                     con = engine,
                                     if_exists = "replace", # replace will drop your table and generate new based on input data
                                     index = False)
        
    def output(self):
        return [luigi.LocalTarget("data/load/db_load_data.csv"),
                luigi.LocalTarget("data/load/scrape_load_data.csv")]


if __name__ == "__main__":
    luigi.build([LoadData()])