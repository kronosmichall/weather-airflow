import json
import os
from datetime import datetime
import requests
import asyncio

API_VERSION  = "1.0.0"

def request_weather(api_key, city_name):
    url = "http://api.weatherapi.com/v1"
    endpoint = "/current.json"
    params = {
        "key": api_key, 
        "q": city_name, 
    }
    
    return asyncio.to_thread(requests.get, url + endpoint, params=params)

def add_meta_data(response, time):
    return response | { "meta_data": { "time": time, "api_version": API_VERSION } }
 
async def request_weather_list(api_key, city_names, time):
    responses = await asyncio.gather(*[request_weather(api_key, city_name) for city_name in city_names])
    responses = [r.json() for r in responses]
    responses = [add_meta_data(r, time) for r in responses]
    return responses

def get_countries():
    file_path = os.path.join(os.path.dirname(__file__), "capitals.json")
    with open(file_path, "r") as f:
        return json.load(f).values()

def extract(api_key, time):
    countries = get_countries()
    return asyncio.run(request_weather_list(api_key, countries, time))