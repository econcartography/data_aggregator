import pandas as pd
import json
import os
import time
import argparse


def get_query(args):
    with open('countries.json','r') as f:
        countries = [c['id'] for c in json.load(f)['results']]
    yy = args.years.split('-')
    years = range(int(yy[0]),int(yy[-1])+1)
    if args.reporter:
        reporters = [args.reporter]
    else:
        index = list(map(int,args.reporter_index.split('-')))
        reporters = countries[index[0]:index[-1]+1]

    data_folder      = os.path.join('Export_data_yearly',args.digits)
    log_file         = 'log_{}'.format(args.digits)
    warnings_file    = 'warnings_{}'.format(args.digits)
    country_ALL_file = 'Export_{year}_{country}_{partner}.csv'
    ## format of the query
    query = "http://comtrade.un.org/api/get?max=50000&fmt={fmt}&freq={freq}&px={px}&r={r}&p={p}&rg={rg}&cc={cc}&type={type}&ps={ps}"
    params = {'fmt':'csv', #data format
              'freq':'A', #frequency, A in annual
              'px':'HS', #classification (HS is the same of ITC)
              'r': '276',#couple[0]['id'], #exporter, 381 is Italy
              'p':'all', #importer
              'ps':'2014,2015,2016,2017,2018',
              'rg':'2', #trade regime, 2 is export
              'cc':'AG2', #classification code, varying the digit makes the classification coarser/finer
              'type':'C'} #type, C is commodity

    for year in years:
        for country in reporters:
            print(os.path.join(data_folder,str(year),country_ALL_file.format(year=year,
                                                                             country=country,
                                                                             partner=args.partner)))
            os.makedirs(os.path.join(data_folder,str(year)),exist_ok=True)
            files = [f.lower() for f in os.listdir(os.path.join(data_folder,str(year)))]
            if country_ALL_file.format(year=year,country=country,partner=args.partner).lower() in files:
                continue
            params.update(dict(r=country,p=args.partner,ps=year,cc=args.digits))
            try:
                df = pd.read_csv(query.format(**params))
                if len(df)==50000 or len(df)==1:
                        with open(warnings_file, 'a') as f:
                            f.write("{},{},{},{}\n".format(country,args.partner,year,len(df)))
                else:
                        df.to_csv(os.path.join(data_folder,str(year),
                                  country_ALL_file.format(year=year,country=country,partner=args.partner)),
                                  index=None)
            except Exception as exc:
                with open(log_file,'a') as log:
                        log.write("{},{},{}\n".format(country,args.partner,year))
                        log.write(str(exc))
                        log.write("\n")
                with open(warnings_file, 'a') as f:
                      f.write("{},{},{}\n".format(country,args.partner,year))
                if "HTTP Error 409" in str(exc):
                    time.sleep(3600)


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    group  = parser.add_mutually_exclusive_group()
    parser.add_argument("-y", "--years", help="year or years to query for. Format: yyyy or yyyy-yyyy", type=str, default='1995-2018')
    parser.add_argument("-d", "--digits", help="Number of digits to retrieve.", type=str, choices=['AG2','AG4']) 
    group.add_argument("-r", "--reporter", help="Country that exports.", type=int)
    group.add_argument("-R", "--reporter-index", help="Range of countries that exports. Max is 0-254", type=str)
    parser.add_argument("-p", "--partner", help='Country tha imports.', type=str, default='all')
    args = parser.parse_args()

    get_query(args)
