import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# https://www.yahoofinanceapi.com/dashboard

import requests

url = "https://yfapi.net/v6/finance/quote"

querystring = {"symbols":"5017.T"}

headers = {
    'x-api-key': "SAzdIpDTOT4evVAsP88EU9NmwKmpAQLP9RsCmzYP"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
