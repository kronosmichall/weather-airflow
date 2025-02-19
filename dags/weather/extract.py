import json
import os
from datetime import datetime
import requests
import asyncio

def request_weather(api_key, city_name):
    url = "http://api.weatherapi.com/v1"
    endpoint = "/current.json"
    params = {
        "key": api_key, 
        "q": city_name, 
    }
    
    return asyncio.to_thread(requests.get, url + endpoint, params=params)

async def request_weather_list(api_key, city_names):
    responses = await asyncio.gather(*[request_weather(api_key, city_name) for city_name in city_names])
    return [response.json() for response in responses]

def get_countries():
    file_path = os.path.join(os.path.dirname(__file__), "capitals.json")
    with open(file_path, "r") as f:
        return json.load(f).values()

def extract(api_key):
    countries = get_countries()
    return asyncio.run(request_weather_list(api_key, countries))