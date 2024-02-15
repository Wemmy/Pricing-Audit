import pandas as pd
from read_data import extract_code, get_all_data

def match_by_code(df_pos, df_list):
    '''
    Index(['sector', 'pos_source', 'cleansed_tier_1', 'cleansed_tier_2',
        'cleansed_tier_3', 'cleansed_tier_4', 'menuitemname', 'singleitemprice',
        'store_province', 'storename', 'plu', 'scancode', 'groupname',
        'categoryname', 'sales', 'quantity', 'Full_Name_Pos', 'Pricing Region'],
        dtype='object')
    Index(['Category', 'Sub-Category', 'Brand', 'Item_Name', 'Item_Code', 'Size',
       'Item_Price', 'Full_Name_List'],
      dtype='object')
    '''
    # only work on list that has single code
    df_list = df_list[~df_list.duplicated(subset=['Item_Code'], keep=False)]

    # Merging on 'scancode'
    merged_df_scancode = pd.merge(df_pos, df_list, left_on='scancode', right_on='Item_Code', how = 'left')
    
    joined_df_scancode = merged_df_scancode[merged_df_scancode['Item_Code'].notnull()]
    # Identify unjoined (unmatched) records - null in the columns from df2
    unjoined_df = merged_df_scancode[merged_df_scancode['Item_Code'].isnull()]

    # Merging on 'plu'
    merged_df_plu = pd.merge(unjoined_df[df_pos.columns], df_list, left_on='plu', right_on='Item_Code', how='left')
    joined_df_plu = merged_df_plu[merged_df_plu['Item_Code'].notnull()]
    unjoined_df = merged_df_plu[merged_df_plu['Item_Code'].isnull()][df_pos.columns]

    # Combine the results of both merges
    combined_merged_df = pd.concat([joined_df_scancode, joined_df_plu]).drop_duplicates(subset=df_pos.columns)

    # # Get the indices of the matched rows
    # matched_indices = combined_merged_df.index
    # # Filter out unmatched records from df1
    # unmatched_df = df_pos[~df_pos.index.isin(matched_indices)]

    return combined_merged_df, unjoined_df

if __name__ == '__main__':
    # get data
    df_list_ONMB, df_list_QC, df_list_others, df_list_ONHVP, df_pos_OM, df_pos_QC, df_pos_others, df_pos_OHVP = get_all_data()
    # print(len(df_list_ONMB['Item_Code']))
    # print(len(df_list_ONMB['Item_Code'].unique()))

    # duplicates_specific_column = df_list_ONMB[df_list_ONMB.duplicated(subset=['Item_Code'])]
    # duplicates_specific_column.to_excel('./Results/test2.xlsx')
    df1, df2= match_by_code(df_pos_OM,df_list_ONMB)
    # print(len(df_pos_OM))
    # print(len(df1))
    # print(len(df2))

def test(df_pos,t_cpg,t_otg,t_foundation):
    (df_list_CPG, UPC_CPG) = t_cpg
    (df_list_OTG, MWC_OTG) = t_otg
    (df_list_Foundation,MWC_Foundation) = t_foundation

    # matching by code (of 100% confidence matching)
    df_upc_cpg = df_pos[df_pos['scancode'].apply(lambda x: x in UPC_CPG)]
    df_upc_cpg_merged = pd.merge(df_upc_cpg, df_list_CPG, how='left', left_on='scancode', right_on='UPC Code').drop_duplicates(subset=df_upc_cpg.columns)
    print('Left match successful? %s' % (len(df_upc_cpg) == len(df_upc_cpg_merged)))
    df_upc_cpg_mapping = df_upc_cpg_merged[['menuitemname', 'Item Description']].drop_duplicates()

    df_mwc_otg = df_pos[df_pos['plu'].apply(lambda x: x in MWC_OTG)]
    df_mwc_otg_merged = pd.merge(df_mwc_otg, df_list_OTG, how='left', left_on='plu', right_on='Menu Works Code').drop_duplicates(subset=df_mwc_otg.columns)
    print('Left match successful? %s' % (len(df_mwc_otg) == len(df_mwc_otg_merged)))
    df_upc_otg_mapping = df_mwc_otg_merged[['menuitemname', 'Full Item Description (EN)']].drop_duplicates()

    df_mwc_foundation = df_pos[df_pos['plu'].apply(lambda x: x in MWC_Foundation)]
    df_mwc_foundation_merged = pd.merge(df_mwc_foundation, df_list_Foundation, how='left', left_on='plu', right_on='Menu Works Code').drop_duplicates(subset=df_mwc_foundation.columns)
    print('Left match successful? %s' % (len(df_mwc_foundation) == len(df_mwc_foundation_merged)))
    df_upc_foundation_mapping = df_mwc_foundation_merged[['menuitemname', 'Full Item Description (EN)']].drop_duplicates()

    name_list_upc_cpg = set([name for name in df_upc_cpg_mapping['menuitemname'] if name != ''])
    name_list_mwc_otg = set([name for name in df_upc_otg_mapping['menuitemname'] if name != ''])
    name_list_mwc_foundation =set([name for name in df_upc_foundation_mapping['menuitemname'] if name != ''])

    upc_all_prematch = UPC_CPG
    MWC_all_prematch = MWC_OTG | MWC_Foundation
    name_all_prematch = name_list_upc_cpg | name_list_mwc_otg | name_list_mwc_foundation

    df_name_cpg = df_pos[df_pos['menuitemname'].apply(lambda x: x in name_list_upc_cpg)]
    df_name_cpg_merged = pd.merge(df_name_cpg, df_upc_cpg_mapping, how='left', left_on='menuitemname', right_on='menuitemname').drop_duplicates(subset=df_name_cpg.columns)
    df_name_cpg_merged = pd.merge(df_name_cpg_merged, df_list_CPG, how='left', left_on='Item Description', right_on='Item Description').drop_duplicates(subset=df_name_cpg.columns)
    df1 = pd.concat([df_upc_cpg_merged, df_name_cpg_merged], ignore_index=True).drop_duplicates(subset = df_pos.columns)

    df_name_otg = df_pos[df_pos['menuitemname'].apply(lambda x: x in name_list_mwc_otg)]
    df_name_otg_merged = pd.merge(df_name_otg, df_upc_otg_mapping, how='left', left_on='menuitemname', right_on='menuitemname').drop_duplicates(subset=df_name_otg.columns)
    df_name_otg_merged = pd.merge(df_name_otg_merged, df_list_OTG, how='left', left_on='Full Item Description (EN)', right_on='Full Item Description (EN)').drop_duplicates(subset=df_name_otg.columns)
    df2 = pd.concat([df_mwc_otg_merged, df_name_otg_merged] , ignore_index=True).drop_duplicates(subset = df_pos.columns)

    df_name_foundation = df_pos[df_pos['menuitemname'].apply(lambda x: x in name_list_mwc_foundation)]
    df_name_foundation_merged = pd.merge(df_name_foundation, df_upc_foundation_mapping, how='left', left_on='menuitemname', right_on='menuitemname').drop_duplicates(subset=df_name_foundation.columns)
    df_name_foundation_merged = pd.merge(df_name_foundation_merged, df_list_Foundation, how='left', left_on='Full Item Description (EN)', right_on='Full Item Description (EN)').drop_duplicates(subset=df_name_foundation.columns)
    df3 = pd.concat([df_mwc_foundation_merged, df_name_foundation_merged] , ignore_index=True).drop_duplicates(subset = df_pos.columns)

    # df1['Category'] = 'CPG'
    df1['Score'] = 1
    col_rename = {'Item Description': 'Item List', 'scancode': 'pos code', 'UPC Code':'list code'}
    df1 = df1.rename(columns = col_rename)

    # df2['Category'] = 'OTG'
    df2['Score'] = 1
    col_rename = {'Full Item Description (EN)': 'Item List', 'plu': 'pos code', 'Menu Works Code':'list code'}
    df2 = df2.rename(columns = col_rename)

    # df3['Category'] = 'Foundation'
    df3['Score'] = 1
    col_rename = {'Full Item Description (EN)': 'Item List', 'plu': 'pos code', 'Menu Works Code':'list code'}
    df3 = df3.rename(columns = col_rename)

    common_columns = ['cleansed_tier_1', 'cleansed_tier_2', 'cleansed_tier_3', 'cleansed_tier_4', 'sector', 'pos_source',
                    'menuitemname', 'singleitemprice', 'store_province', 'storename', 'pos code', 'groupname', 
                    'sales', 'quantity', 'Item List',  'list code', 'Summer 2023 SRP', 'Category', 'Sub-Category', 'Score']

    df_codematched = pd.concat([df1[common_columns], df2[common_columns], df3[common_columns]], ignore_index=True).drop_duplicates(subset=['cleansed_tier_1', 'cleansed_tier_2', 'cleansed_tier_3', 'cleansed_tier_4', 'sector', 'pos_source',
                    'menuitemname', 'singleitemprice', 'store_province', 'storename', 'pos code', 'groupname'])
    # df_codematched = df_codematched[common_columns]
    df_codematched['Matche Type'] = 'Code Match'

    return df_codematched, upc_all_prematch, MWC_all_prematch, name_all_prematch