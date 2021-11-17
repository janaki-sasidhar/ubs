import os
import pandas as pd


df = pd.read_csv('ubs_updated.csv')

location = pd.read_csv('location.csv')


triplets = pd.read_csv('triplets.csv' , keep_default_na=False,  na_values=['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A N/A', '#N/A', 'N/A', 'n/a',  '<NA>', '#NA', 'NULL', 'null', 'NaN', '-NaN', 'nan', '-nan', ''])


triplets_1= triplets[triplets['ISIN'].notnull()]


triplets_2 = triplets_2[triplets_2['ISIN'].notnull()]
location_2 = triplets_2[triplets_2['Location'].notnull()]



df_as_dict = df.to_dict('records') # faster 


location_data = dict()

for index , row in location.iterrows():
    comb_string = row['SPONSOR INPUT VALUE 1'] + row['SPONSOR INPUT VALUE 2']
    location_data[comb_string] = row['TRIPLETS MATCHING VALUE']

print(location_data)

#
#for index , row in ubs.iterrows():
#    isin_two = row['etf_isin'][0:2]
#    local_currency = row['local_currency']
#    comb_string = isin_two + local_currency
#    ubs.loc[index , 'location'] = data.get(comb_string)
#


def triplets_by_only_isin():
    temp_dict = dict()
    for row in triplets_1.itertuples():

        key_ = f"{row.ISIN}"
        value_ = [ row.ConstituentTicker ,
                   row.ConstituentType ,
                   row.ISIN ,
                   row.CUSIP,
                   row.Location ,
                   row.SEDOL]
        temp_dict[key_] = value_
    t2 = time.perf_counter()
    return temp_dict


triplets_isin = triplets_by_only_isin()


def triplets_by_isin_and_location():
    t1 = time.perf_counter()
    temp_dict = dict()
    for row in triplets_2.itertuples():

        key_ = f"{row.ISIN}@@@{row.Location}"
        value_ = [ row.ConstituentTicker ,
                   row.ConstituentType ,
                   row.ISIN ,
                   row.CUSIP,
                   row.Location ,
                   row.SEDOL]
        temp_dict[key_] = value_
    t2 = time.perf_counter()
    print(f'time diff is {t2-t1}')
    return temp_dict

triplets_isin_location = triplets_by_isin_and_location()



for index , item in enumerate(df_as_dict):
    print(index)
    if item['constituent_type'] == 'BOND':
        # update location first
        # combo 
        combined =  item['etf_isin'][0:2] + item['local_currency']
        item['location'] = location_data.get(combined)
        isin_in_dict = item['etf_isin'] # isin here is etf_isin 

        # triplets by isin get
        triplets_usage_isin = triplets_isin.get(isin_in_dict)
        if triplets_usage_isin:
            item['constituent_ticker'] = triplets_usage_isin[0]
            item['constituent_type'] = triplets_usage_isin[1]
            item['cusip'] = triplets_usage_isin[3]
            item['location'] = triplets_usage_isin[4]
            item['sedol'] = triplets_usage_isin[5]

    else:
        if str(item['etf_isin']) != 'nan' and str(item['location']) != 'nan':
            key_to_search = f'{item['etf_isin']}@@@{row['location']}'
            triplets_usage_isin_location = triplets_isin_location.get(key_to_search)
            if triplets_usage_isin_location:
                item['constituent_ticker'] = triplets_usage_isin_location[0]
                item['constituent_type'] = triplets_usage_isin_location[1]
                item['cusip'] = triplets_usage_isin_location[3]
                item['location'] = triplets_usage_isin_location[4]
                item['sedol'] = triplets_usage_isin_location[5]




#empty dataframe 
#columns = df.columns
new_df = pd.DataFrame(df_as_dict)
new_df.to_csv('alldone.csv')



       
