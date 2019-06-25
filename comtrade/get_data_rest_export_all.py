import comtrade_API_v2 as API
import json
import itertools
import pandas as pd


class Args:
    years  = "2005-2017"
    digits = "AG4"

    def __init__(self,r,p):
        self.reporter=r
        self.partner=p

with open("rest_countries.json") as f:
    countries = [c['id'] for c in json.load(f)['results']]

#countries = countries[-100:]

# correlation matrix
for r in countries:
    args = Args(r,'0')
    API.get_query(args)

