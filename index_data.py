from elastic_enterprise_search import AppSearch
import json
import argparse
from elasticsearch import Elasticsearch, helpers


parser = argparse.ArgumentParser()
parser.add_argument('--app_search_url', dest='app_search_url', required=True)
parser.add_argument('--app_search_private_key', dest='app_search_private_key', required=True)
args = parser.parse_args()

app_search = AppSearch(
    args.app_search_url,
    http_auth=args.app_search_private_key
)

print("Create engine cities...")
# Create engine
app_search.create_engine(
    engine_name="cities",
    language="en"
)

print("Engine created")
print ("Load data...")
# Load data
with open("./data/cities.json") as f:
    jsonFile = json.load(f)
    products = []
    i = 0
    for obj in jsonFile:
        products.append(obj)
        i = i + 1
        if i > 99:
            app_search.index_documents(engine_name="cities",documents=products,request_timeout=60)
            i = 0
            products = []
            print(".")

print ("Data loaded")        
print ("Update schema..")
# # Update schema
app_search.put_schema(
    engine_name="cities",
    schema={
        "coordinates": "geolocation",
        "population": "number"
    }
)

app_search.put_search_settings(
    engine_name="cities",
    boosts={"population": [{"type":"functional","factor":1.5,"function":"linear","operation":"add"}]},
    precision=8
)

print("Create engine stores ...")
# Create engine
app_search.create_engine(
    engine_name="stores",
    language="en"
)

print("Engine created")
print ("Load data...")
# Load data
with open("./data/stores.json") as f:
    jsonFile = json.load(f)
    products = []
    i = 0
    for obj in jsonFile:
        products.append(obj)
        i = i + 1
        if i > 99:
            app_search.index_documents(engine_name="stores",documents=products,request_timeout=60)
            i = 0
            products = []
            print(".")

print ("Data loaded")        
print ("Update schema..")
# # Update schema
app_search.put_schema(
    engine_name="stores",
    schema={
        "location": "geolocation"
    }
)

print ("Done")   