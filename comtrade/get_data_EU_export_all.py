import comtrade_API_v2 as API
import json
import itertools



class Args:
    years  = "2018"
    digits = "AG2"

    def __init__(self,r,p):
        self.reporter=r
        self.partner=p

with open("pers_countries.json") as f:
    countries = [c['id'] for c in json.load(f)['results']]

#countries = ['470']

# correlation matrix
for r in countries:
    args = Args(r,'0')
    API.get_query(args)

