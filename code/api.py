import requests
from requests.structures import CaseInsensitiveDict

url = "https://api.geoapify.com/v1/geocode/reverse?lat=49.26269431856397&lon=-122.74820972266127&apiKey=afd572b5a5414cc58c98b25e8ce47fb7"

resp = requests.get(url)
print(resp.content)