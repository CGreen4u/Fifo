import pandas as pd
import numpy as np


class fifo_des:
    def __init__(self, filename):
        self.filename = filename
        self.df = pd.DataFrame()
        self.df2 = pd.DataFrame()
        self.df3 = pd.DataFrame()
        self.new_df = pd.DataFrame()
        self.df['value_earn'] = 0
        self.df['value_burn'] = 0
        self.goget()
        self.pace()
        self.data_moves()
        self.fifo()
        self.clean_up()
        

    def goget(self):
        for index, row in self.df.iterrows():
            if row['transactionType'] == 'earn_air':
                self.df['value_earn'][index] = row['value']
            elif row['transactionType'] == 'earn_other':
                self.df['value_earn'][index] = row['value']
            elif row['transactionType'] == 'earn_lodging':
                self.df['value_earn'][index] = row['value']
            elif row['transactionType'] == 'earn_package':
                self.df['value_earn'][index] = row['value']
            elif row['transactionType'] == 'earn_credit_card':
                self.df['value_earn'][index] = row['value']
            elif row['transactionType'] == 'rdm_package':
                self.df['value_burn'][index] = row['value']
            elif row['transactionType'] == 'rdm_lodging':
                self.df['value_burn'][index] = row['value']
            elif row['transactionType'] == 'exp':
                self.df['value_burn'][index] = row['value']
            else: 
                pass
        return self.df
    
    def pace(self):
        '''seperate out the information'''
        for index, row in df.iterrows():
            #print('x')
            if row['value_burn'] == 0:
                self.df2 = self.df2.append(row)
                #df.drop(df.index[index])
            elif row['value_earn'] == 0:
                self.df3 = self.df3.append(row)

    def data_moves(self):
        '''This function is designed to group data in a data frame for transactions'''
        self.df = pd.read_csv(self.filename) #bring file to play
        self.df['transactionType'] = self.df['transactionType'].astype("string") #convert types
        self.df.head()
        dd1 = df3.groupby(['id', 'transactionDate','transactionType']).mean().reset_index(drop=False)
        ddo = self.df2.groupby(['id','transactionDate','transactionType']).mean().reset_index(drop=False) #group data
        self.new_df=ddo.merge(dd1, how='left', on='id' ).reset_index(drop=True) #combine data
        return self.new_df
    
    def fifo(self):
        '''This function is the fifo function I was a little confused on how it actually works'''
        self.new_df['fifo'] = None
        for index, row in self.new_df.iterrows(): #iterate over rows in dataframe
            x = (row['value_earn_x']/2) - row['value_burn_y'] #subtraction for fifo not in use
            x #add this somewhere else once you figure out the formula
            self.new_df['fifo'][index] = row['value_earn_x']/2 #fifo formula
        return self.new_df
    
    def swaper_3000(columns, typ=str):
        '''covert data types'''
        for column in columns:
            new_df[column] = new_df[column].astype(typ)
        return self.new_df

    def clean_up(self):
        '''clean up the data frame'''
        column_int64 = ['id', 'transactionID_earn','value_earn','transactionID_burn','value_burn','fifo' ]
        column_date = ['transactionDate_earn', 'transactionDate_burn']
        column_obj = ['transactionType_burn', 'transactionType_earn']
        self.new_df.drop(['value_x', 'value_burn_x', 'value_y', 'value_earn_y'], axis = 1, inplace=True) #drop unwanted data
        new_df.rename(columns={"transactionDate_x": "transactionDate_earn", "transactionType_x": "transactionType_earn", 
                               "transactionID_x": "transactionID_earn", "value_earn_x": "value_earn", 
                               "transactionDate_y": "transactionDate_burn", "transactionType_y": "transactionType_burn", 
                               "value_burn_y": "value_burn", "fifo": "fifo", "transactionID_y":"transactionID_burn"}, inplace = True)
        swaper_3000(column_int64, 'int64')
        swaper_3000(column_date, 'datetime64[ns]')
        swaper_3000(column_obj, 'object')
        return self.new_df
    
