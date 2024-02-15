import os
import pandas as pd
from read_data import *
from Match_by_Code import match_by_code
from utils import *
import sqlalchemy
from Item_Classification import MyCorpus

def preprocess(df_pos, df_list):

    df_list[['Full_Name_List','Size_List']]  = df_list['Full_Name_List'].apply(lambda x: pd.Series(data_cleaning(x)))
    df_pos[['Full_Name_Pos','Size_Pos']]  = df_pos['Full_Name_Pos'].apply(lambda x: pd.Series(data_cleaning(x)))

    # # remove unuseful columns
    # df_pos = df_pos[['cleansed_tier_1', 'cleansed_tier_2', 'cleansed_tier_3','cleansed_tier_4', 
    #                  'sector', 'pos_source', 'storename', 'store_province', 'groupname',
    #                  'menuitemname', 'singleitemprice', 'sales', 'quantity']]

    return df_pos,df_list


    # rename and combine
    # df_list_CPG_post = df_list_CPG_post[['Full_Name', 'Size', 'Summer 2023 SRP', 'Category', 'Sub-Category']]
    # df_list_CPG_post['class'] = 'CPG'
    # df_list_OTG_post = df_list_OTG_post[['Full_Name', 'Size', 'Summer 2023 SRP', 'Category', 'Sub-Category']]
    # df_list_OTG_post['class'] = 'OTG'
    # df_list_Foundation_post = df_list_Foundation_post[['Full_Name', 'Size', 'Summer 2023 SRP', 'Category', 'Sub-Category']]
    # df_list_Foundation_post['class'] = 'Foundation'
    # df_list_post = pd.concat([df_list_CPG_post, df_list_OTG_post, df_list_Foundation_post], axis=0)
def output_excel(df_list, df_pos, df_code_match, file_name = None):
    # my corpus class
    mc = MyCorpus(data_train = df_list, data_test = df_pos)
    mc.create_dictionary()
    mc.create_model()
    mc.create_index_text()
    mc.find_best_match()
    mc.construct_result()
    mc.result['match type'] = 'name'
    df_code_match['match type'] = 'code'
    df_code_match['similarity'] = 1
    result = pd.concat([mc.result[df_code_match.columns], df_code_match])
    result.to_excel(f"Results\{file_name}.xlsx", index=False)
    print(f'{file_name} in excel done')


if __name__ == "__main__":
    # change working dir
    os.chdir('E:\Work\Pricing\Pricing Audit')

    # get data
    df_list_ONMB, df_list_QC, df_list_others, df_list_ONHVP, df_pos_OM, df_pos_QC, df_pos_others, df_pos_OHVP = get_all_data()
    # print(df_list_ONMB.columns)
    # print(df_pos_OM.columns)

    df_OM_code_match, df_OM_code_unmatch = match_by_code(df_pos_OM,df_list_ONMB)
    print('Left match successful? %s' % (len(df_pos_OM) == (len(df_OM_code_match) + len(df_OM_code_unmatch))))
    df_QC_code_match, df_QC_code_unmatch = match_by_code(df_pos_QC,df_list_QC)
    print('Left match successful? %s' % (len(df_pos_QC) == (len(df_QC_code_match) + len(df_QC_code_unmatch))))
    df_Others_code_match, df_Others_code_unmatch = match_by_code(df_pos_others,df_list_others)
    print('Left match successful? %s' % (len(df_pos_others) == (len(df_Others_code_match) + len(df_Others_code_unmatch))))
    df_ONHVP_code_match, df_ONHVP_code_unmatch = match_by_code(df_pos_OHVP,df_list_ONHVP)
    print('Left match successful? %s' % (len(df_pos_OHVP) == (len(df_ONHVP_code_match) + len(df_ONHVP_code_unmatch))))

    # # preprocess
    # df_list_post, df_pos_post = preprocess(df_pos, t_cpg[0], t_otg[0], t_foundation[0], upc_all_prematch, MWC_all_prematch, name_all_prematch)
    df_pos_OM, df_list_ONMB = preprocess(df_OM_code_unmatch, df_list_ONMB)
    df_pos_QC, df_list_QC = preprocess(df_QC_code_unmatch, df_list_QC)
    df_pos_others, df_list_others = preprocess(df_Others_code_unmatch, df_list_others)
    df_pos_OHVP, df_list_ONHVP = preprocess(df_ONHVP_code_unmatch, df_list_ONHVP)

    fp = 'Jan13_Feb09'
    output_excel(df_list_ONMB, df_pos_OM, df_OM_code_match, file_name = f'Item_Pricing_Audit_ONMN_{fp}')
    output_excel(df_list_QC, df_pos_QC, df_QC_code_match, file_name = f'Item_Pricing_Audit_QC_{fp}')
    output_excel(df_list_others, df_pos_others, df_Others_code_match, file_name = f'Item_Pricing_Audit_Others_{fp}')
    output_excel(df_list_ONHVP, df_pos_OHVP, df_ONHVP_code_match, file_name = f'Item_Pricing_Audit_ONHVP_{fp}')

    # # to sql
    # azSqlConn = sqlalchemy.create_engine(f"mssql+pyodbc://CAWPAPP3464/CGC?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes", fast_executemany = True)
    # df_final_result.to_sql(f"Item_Pricing_Audit_July1_July14",schema = 'Analytics',con = azSqlConn, if_exists ='replace',index = False)
    # print('Result in SQL done')

    

    