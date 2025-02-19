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

def request_weather_list(api_key, city_names):
    return asyncio.gather(*[request_weather(api_key, city_name) for city_name in city_names])

def get_countries():
    with open("capitals.json", "r") as f:
       return json.load(f).values()

async def extract(api_key):
    countries = get_countries()
    result = await request_weather_list(api_key, countries)