from read_data import get_audit_transaction,get_storename_mapping
import pandas as pd
import matplotlib.pyplot as plt


def mapout_distribution(df):
    # Plotting
    plt.figure(figsize=(10, 6))
    plt.bar(df['menuitemname'], df['sales'])
    plt.xlabel('Menu Item Name')
    plt.ylabel('Total Sales')
    plt.title('Sales Distribution of Each Menu Item')
    plt.xticks(rotation=45)  # Rotate labels for better readability if needed
    plt.show()

def cut_out_top(df, p):
    # cut out trivial items
    total_sales = df['sales'].sum()
    # Calculate the cumulative sum of sales
    df['Cumulative Sales'] = df['sales'].cumsum()
    # Find the products that constitute 75% of the overall sales
    cutout_value = total_sales * p
    return df[df['Cumulative Sales'] <= cutout_value]
     

if __name__ == '__main__':
    df = get_audit_transaction()
    # temporary store it in a local file
    # Save to a pickle file
    df.to_pickle('data/data_tmp.pkl')

    # # # Load the DataFrame from the pickle file
    # df_trans = pd.read_pickle('data/data_tmp.pkl')
    
    # # without clear view and Tims
    # df_trans = df_trans[(df_trans['pos_source'] != 'clearview') & (df_trans['groupname'] != 'Tim Hortons')]
    # # without null in menuitemname
    # df_trans = df_trans[df_trans['menuitemname']!= None]

    # # mapping pricing tag based on storename
    # df_mapping = get_storename_mapping()
    # df_trans = df_trans.merge(df_mapping, on = 'storename')
    
    # # group by pricing plan
    # df_trans_AOP = df_trans[df_trans['Pricing Region'] == 'ALL OTHER PROVINCES']
    # df_trans_OM = df_trans[df_trans['Pricing Region'] == 'ONTARIO & MANITOBA']
    # df_trans_OHVR = df_trans[df_trans['Pricing Region'] == 'Ontario Hospitals Volante Pricing']
    # df_trans_QC = df_trans[df_trans['Pricing Region'] == 'QUEBEC']

    # # basic info
    # print(f"AOP Size: {df_trans_AOP.shape[0]}; size of Dstinct items: {df_trans_AOP['menuitemname'].nunique()}" )
    # print(f"OM Size: {df_trans_OM.shape[0]}; size of Dstinct items: {df_trans_OM['menuitemname'].nunique()}" )
    # print(f"OHVR Size: {df_trans_OHVR.shape[0]}; size of Dstinct items: {df_trans_OHVR['menuitemname'].nunique()}" )
    # print(f"QC Size: {df_trans_QC.shape[0]}; size of Dstinct items: {df_trans_QC['menuitemname'].nunique()}" )

    # df_sum = df_trans_OM.groupby(['menuitemname','groupname',  'plu', 'scancode'])['sales'].sum().reset_index().sort_values(by='sales', ascending=False)
    # print(df_sum.head(100))
    # map out distribution of items for each group
    # p = 0.75
    # mapout_distribution(df_trans_AOP)
    # df_filtered = cut_out_top(df_trans_AOP, p)
    # mapout_distribution(df_filtered)

    

   
    

