import comtrade_API_v2 as API
import json
import itertools



class Args:
    years  = "1995-2018"
    digits = "AG2"

    def __init__(self,r,p):
        self.reporter=r
        self.partner=p

with open("EU_countries.json") as f:
    countries = [c['id'] for c in json.load(f)['results']]

# correlation matrix
#for r in countries:
#    args = Args(r,'0')
#    API.get_query(args)

# multlayer
for r,p in itertools.product(countries,countries):
    if r==p: continue
    args = Args(r,p)
    API.get_query(args)

