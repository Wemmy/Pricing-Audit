from utils import *
import connectorx as cx
import pandas as pd
import re
from dotenv import load_dotenv
import os

load_dotenv()

with open('data.sql', 'r') as file:
    SQL = file.read()

def get_audit_transaction():
    conn = os.getenv('CONN_REDSHIFT')
    df_pos = cx.read_sql(conn, SQL)
    print('total records: %d' % len(df_pos))
    return df_pos.copy() 

def get_pricing_list():
    # list data
    df_list_CPG = read_excel_file('./Data/Pricing_Lists.xlsx', sheet_name = 'CPG SRP', header_row = 1)
    df_list_OTG = read_excel_file('./Data/Pricing_Lists.xlsx', sheet_name = 'Coffee, OTG,Internal Brands SRP', header_row = 1)  
    df_list_Foundation = read_excel_file('./Data/Pricing_Lists.xlsx', sheet_name = 'Foundations SRP', header_row = 1)

    # normalize column name
    cols_cpg_map = {'Item Description': 'Item_Name', 'UPC Code': 'Item_Code',\
               'Ontario & manitoba': 'ON&MB','QUEBEC':'QC','ALL OTHER PROVINCES':'Others','Ontario Hospitals Volante Pricing':'ONHVP'}
    cols_otg_map = {'Full Item Description (EN)': 'Item_Name', 'Internal Brands':'Brand', 'Menu Works Code':'Item_Code',\
               'ONTARIO & MANITOBA':'ON&MB', 'QUEBEC':'QC', 'ALL OTHER PROVINCES':'Others','Ontario Hospitals Volante Pricing':'ONHVP'}
    cols_foundation_map = { 'Foundation Station':'Brand', 'Full Item Description (EN)': 'Item_Name', 'Menu Works Code': 'Item_Code',\
               'Ontario & Manitoba':'ON&MB','Quebec':'QC','All Other Provinces':'Others','Ontario Hospitals Volante Pricing':'ONHVP'}
    df_list_CPG.rename(columns=cols_cpg_map, inplace= True)
    df_list_OTG.rename(columns=cols_otg_map, inplace= True)
    df_list_Foundation.rename(columns=cols_foundation_map, inplace= True)

    # select useful columns
    cols = {'Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size','ON&MB','QC','Others','ONHVP'}
    
    return df_list_CPG[cols], df_list_OTG[cols], df_list_Foundation[cols]

def get_storename_mapping():
    cols_name = ['storename', 'Pricing Region']
    df = read_table('./Data/Store_Sector.xlsx', table_name = 'Table1')
    return df[cols_name]


def contains(text, pattern = r'\bBeverage|\bBev\b|\bPromotions\b'):
    return re.search(pattern, text, re.IGNORECASE)


def Inital_Cleaning_Pricing_List(df):
    # convert invalid price to 0
    df['Item_Price'] = df['Item_Price'].apply(lambda x: 0 if isinstance(x, str) else x)
    df['Item_Price'] = df['Item_Price'].fillna(0).replace('', 0)

    # we don't care about discount and 0
    df = df[df['Item_Price'] > 0]

    # formulate the datatype
    # df['Item_Code'] = df['Item_Code'].fillna('').astype(str)
    # df['Item_Name'] = df['Item_Name'].fillna('').astype(str)

    # Combine brand into item name
    df['Full_Name_List'] = df.apply(lambda row: 'coffee ' + row['Item_Name'] 
                                    if 'Starbucks WPS' in row['Item_Name']
                                    else row['Item_Name'], axis = 1
                                    )
    
    # add brand
    df['Full_Name_List'] = df.apply(lambda row: str(row['Brand']) + ' ' + row['Full_Name_List'] 
                                    if str(row['Brand']).lower() not in str(row['Full_Name_List']).lower()
                                    else row['Full_Name_List'], axis = 1
                                    )
    
    # add size
    df['Full_Name_List'] = df.apply(lambda row: row['Full_Name_List'] + ' ' + str(row['Size'])
                                    if ((re.findall(r'\d+(?:\.\d+)?', str(row['Size']))[0] if re.findall(r'\d+(?:\.\d+)?', str(row['Size'])) else '') not in row['Full_Name_List']) 
                                    else row['Full_Name_List'], axis=1
                                    )
    
    df.reset_index(drop=True, inplace=True)
    
    return df

def Inital_Cleaning_Data_Pos(df_pos):
    # without clear view and Tims
    df_pos = df_pos[(df_pos['pos_source'] != 'clearview') & (df_pos['groupname'] != 'Tim Hortons')]
    # # without null in menuitemname
    # df_pos = df_pos[df_pos['menuitemname']!= None]
    
    # fill na price as 0 
    df_pos['singleitemprice'] = df_pos['singleitemprice'].fillna(0)
    # only care about price >0
    df_pos = df_pos[df_pos['singleitemprice'] > 0]

    # fillna
    # df_pos['scancode'] = df_pos['scancode'].fillna('').astype(str)
    df_pos['menuitemname'] = df_pos['menuitemname'].fillna('').astype(str)
    # df_pos['plu'] = df_pos['plu'].fillna('').astype(str)
    
    # if name starts with 'No', delete those item
    df_pos = df_pos[~df_pos['menuitemname'].str.contains('^no\s', case = False)]
    
    # SPECIAL rule: 'Small Fries':delete code and change name to 'Regular Fries'
    df_pos.loc[df_pos['menuitemname'] == 'Small Fries', 'menuitemname'] = 'Regular Fries'
    df_pos.loc[df_pos['menuitemname'] == 'Regular Fries', 'scancode'] = ''
    df_pos.loc[df_pos['menuitemname'] == 'Regular Fries', 'plu'] = ''

    # specific rules: for OTG and Foundation
    # Internal Brands IN ('Coffee Conspiracy','STARBUCKS WPS', MARKET COFFEE) ;
    # 20oz -> LG; 16oz -> MD; 12oz ->SM; lg->large; md -> medium; sm->small
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: 'CONSPIRACY' + ' ' + row['menuitemname'] 
                                if str(row['groupname']) =='Coffee Conspiracy'
                                else row['menuitemname'], axis = 1
                                )
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: str(row['groupname']) + ' ' + row['Full_Name_Pos'] 
                                if str(row['groupname']) in ('Starbucks - WPS','Starbucks WPS', 'Market Coffee', 'Roots & Seeds', 'Grill & Co', 'San Marzano','Revolution Noodle', 'Bok Choy', 'Chef`s Table','Soup & Chili','Grill Breakfast','Freshly Baked', 'Chop`d and Wrap`d')
                                else row['Full_Name_Pos'], axis = 1
                                )
    # GROUPNAME otg
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: 'OTG' + ' ' + row['Full_Name_Pos'] 
                                if 'OTG' in str(row['groupname'])
                                else row['Full_Name_Pos'], axis = 1
                                )
    # GROUPNAME C+B
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: 'C+B' + ' ' + row['Full_Name_Pos'] 
                                if 'C+B' in str(row['groupname'])
                                else row['Full_Name_Pos'], axis = 1
                                )

    # general rules:
    # add categoryname except contains ('Beverage', 'Bev','Promotion')
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: str(row['categoryname']) + ' ' + row['Full_Name_Pos'] 
                                if str(row['groupname']) not in row['Full_Name_Pos'] and not contains(str(row['categoryname']))
                                else row['Full_Name_Pos'], axis = 1
                                )

    # make sure we have add on item descrpition
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: 'Add ' + row['Full_Name_Pos']
                                    if (row['cleansed_tier_2'] in ['Beverage Add-Ons', 'Sides/Add-Ons'] and not re.search(r'^add\b', row['Full_Name_Pos'].lower())) 
                                    else row['Full_Name_Pos'], axis=1
                                    )

    # SPECIAL rule: replace 'Tall' -> SM, 'Grande' -> MD, 'Grande': 'MD', 
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: row['Full_Name_Pos'].replace('Tall', 'SM')
                                           if str(row['groupname']) in ('Starbucks - WPS','Starbucks WPS')
                                           else row['Full_Name_Pos'], axis=1
                                           )
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: row['Full_Name_Pos'].replace('Venti', 'LG')
                                           if str(row['groupname']) in ('Starbucks - WPS','Starbucks WPS')
                                           else row['Full_Name_Pos'], axis=1
                                           )
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: row['Full_Name_Pos'].replace('Grande', 'MD')
                                           if str(row['groupname']) in ('Starbucks - WPS','Starbucks WPS')
                                           else row['Full_Name_Pos'], axis=1
                                           )

    # SPECIAL rule: 'Cappuccino' 'Latte' 'Americano' 'Mocha' 'Frappuccino': add coffee
    df_pos['Full_Name_Pos'] = df_pos.apply(lambda row: row['Full_Name_Pos'] + ' coffee'
                                           if str(row['groupname']) in ('Starbucks - WPS','Starbucks WPS') and 
                                           ('Cappuccino' in row['Full_Name_Pos'] 
                                            or 'Latte' in row['Full_Name_Pos'] 
                                            or 'Americano' in row['Full_Name_Pos'] 
                                            or 'Frappuccino' in row['Full_Name_Pos'] 
                                            or 'Mocha' in row['Full_Name_Pos']
                                           )
                                           else row['Full_Name_Pos'], axis=1
                                           )
    df_pos.reset_index(drop=True, inplace=True)
    return df_pos


def construct_pricing_list_by_province(df1, df2, df3):
    
    cols_on_mb = ['Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size','ON&MB']
    cols_qc = ['Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size','QC']
    cols_others = ['Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size','Others']
    cols_onhvp = ['Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size','ONHVP']

    df_ONMB = pd.concat([df1[cols_on_mb], df2[cols_on_mb], df3[cols_on_mb]], ignore_index=True)
    df_QC = pd.concat([df1[cols_qc], df2[cols_qc], df3[cols_qc]], ignore_index=True)
    df_others = pd.concat([df1[cols_others], df2[cols_others], df3[cols_others]], ignore_index=True)
    df_ONHVP = pd.concat([df1[cols_onhvp], df2[cols_onhvp], df3[cols_onhvp]], ignore_index=True)

    df_ONMB.rename(columns={'ON&MB':'Item_Price'},inplace=True)
    df_QC.rename(columns={'QC':'Item_Price'},inplace=True)
    df_others.rename(columns={'Others':'Item_Price'},inplace=True)
    df_ONHVP.rename(columns={'ONHVP':'Item_Price'},inplace=True)

    return  df_ONMB, df_QC, df_others, df_ONHVP

def construct_transactions_by_province(df_pos, df_map):
    # join the mapping
    df_pos = df_pos.merge(df_map, on = 'storename', how= 'inner')

    # group by pricing plan
    df_pos_others = df_pos[df_pos['Pricing Region'] == 'ALL OTHER PROVINCES']
    df_pos_OM = df_pos[df_pos['Pricing Region'] == 'ONTARIO & MANITOBA']
    df_pos_OHVP = df_pos[df_pos['Pricing Region'] == 'Ontario Hospitals Volante Pricing']
    df_pos_QC = df_pos[df_pos['Pricing Region'] == 'QUEBEC']

    df_pos_others.reset_index(drop=True, inplace=True)
    df_pos_OM.reset_index(drop=True, inplace=True)
    df_pos_OHVP.reset_index(drop=True, inplace=True)
    df_pos_QC.reset_index(drop=True, inplace=True)
    
    return df_pos_OM, df_pos_QC, df_pos_others, df_pos_OHVP


def extract_code(df):
    code = set([code for code in df['Item_Code'] if code != ''])
    return code

def output_new_stores(df_pos, df_map):
    # Find store names in df_pos that are not in df_map
    df_result = df_pos.loc[~df_pos['storename'].isin(df_map['storename'])].groupby(['storename', 'sector', 'store_province'])['sales'].sum().reset_index()
    # Output the result to an Excel file
    df_result.to_excel('Results/New_Stores.xlsx', index=False)

def get_all_data():
    df1,df2,df3 = get_pricing_list()

    df_ONMB, df_QC, df_others, df_ONHVP= construct_pricing_list_by_province(df1,df2,df3)

    df_list_ONMB = Inital_Cleaning_Pricing_List(df_ONMB)
    df_list_QC = Inital_Cleaning_Pricing_List(df_QC)
    df_list_others = Inital_Cleaning_Pricing_List(df_others)
    df_list_ONHVP = Inital_Cleaning_Pricing_List(df_ONHVP)

    df_pos = get_audit_transaction()
    # Load the DataFrame from the pickle file
    # df_pos = pd.read_pickle('data/data_tmp.pkl')

    # identify any new store in df_pos but not in df_map

    df_pos = Inital_Cleaning_Data_Pos(df_pos)
    df_map = get_storename_mapping()
    output_new_stores(df_pos, df_map)
    df_pos_OM, df_pos_QC, df_pos_others, df_pos_OHVP = construct_transactions_by_province(df_pos, df_map)

    return df_list_ONMB, df_list_QC, df_list_others, df_list_ONHVP, df_pos_OM, df_pos_QC, df_pos_others, df_pos_OHVP

if __name__ == '__main__':
    pass

    
                                








    