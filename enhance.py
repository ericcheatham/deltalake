from __future__ import annotations

import os

import requests

VALIDATION_API_URL = "https://addressvalidation.googleapis.com/v1:validateAddress?key="
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY").strip()


class GeoLocation:
    def __init__(self, lat, lon):
        self.latitude = lat
        self.longitude = lon


def get_geo_location_from_postcode(postcode: str) -> GeoLocation:
    response = requests.get(
        url=VALIDATION_API_URL + GOOGLE_KEY,
    )

    geocode = response.json().get("person").get("geocode")

    return GeoLocation(
        lat=geocode.get("location").get("latitude"),
        lon=geocode.get("location").get("longitude"),
    )
