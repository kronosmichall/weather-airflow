import json

import pandas as pd

def parse_json_simple(response):
    return {
        "time": response["meta_data"]["time"],
        "city": response["location"]["name"],
        "region": response["location"]["region"],
        "country": response["location"]["country"],
        "temp_c": response["current"]["temp_c"],
        "temp_f": response["current"]["temp_f"],
        "wind_mph": response["current"]["wind_mph"],
        "wind_kph": response["current"]["wind_kph"],
        "cloud": response["current"]["cloud"],
        "condition": response["current"]["condition"]["text"],
    }
    
def parse_json_all(response):
    return {
        "time": response["meta_data"]["time"],
        "city": response["location"]["name"],
        "region": response["location"]["region"],
        "country": response["location"]["country"],
        "lat": response["location"]["lat"],
        "lon": response["location"]["lon"],
        "tz_id": response["location"]["tz_id"],
        "localtime_epoch": response["location"]["localtime_epoch"],
        "localtime": response["location"]["localtime"],
    } | {k: v for k, v in response["current"].items() if k != "condition"}

def save_to_csv(data, file_path):
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    
def parse_file_simple(file_path):
    with open(file_path, "r") as f:
        response = json.loads(f.read())
        result = [parse_json_simple(r) for r in response]
        result = pd.DataFrame(result).to_dict(orient="records")
        return result
    
def parse_file_all(file_path):
    with open(file_path, "r") as f:
        response = json.loads(f.read())
        return [parse_json_all(r) for r in response]