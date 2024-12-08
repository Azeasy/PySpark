import geohash

from env import API_KEY, BASE_URL

import requests


def cache_response(func):
    cache = {}

    def wrapper(city):
        if city in cache:
            # Using cached result for city
            return cache[city]

        # Fetching result for city
        result = func(city)
        cache[city] = result
        return result

    return wrapper


@cache_response
def fetch_coordinates(city):
    if city:
        response = requests.get(BASE_URL, params={'q': city, 'key': API_KEY})
        data = response.json()

        if data.get('results'):
            lat = data['results'][0].get('geometry', {}).get('lat')
            lng = data['results'][0].get('geometry', {}).get('lng')
            return lat, lng
    return None, None


def fetch_lat(city):
    return fetch_coordinates(city)[0]


def fetch_lng(city):
    return fetch_coordinates(city)[1]


def get_geohash(lat, lng):
    lat, lng = float(lat), float(lng)
    ghash = geohash.encode(lat, lng, precision=4)
    return ghash
