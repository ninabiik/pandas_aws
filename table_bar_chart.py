#Commented out items for AWS configuration
# import pyarrow as pa
# import pyarrow.parquet as pq
import os
import re
import sys
import pandas as pd
import csv
import xlrd
# import boto3
# import s3fs
# from boto3.dynamodb.conditions import Key, Attr
# from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO)
logging.info('Reading data from Landing S3 bucket')

# this will dicate the s3 location to read file
# key = '{key}'
# bucket = '{bucket_name}'

# this will read the file from S3
# s3 = boto3.client('s3')
# obj = s3.get_object(Bucket=bucket, Key=key)
# filedata = obj['Body'].read()


file_loc = {'src_dir': r'{source_directory}',
            'target_dir': r'{target_directory}',
            'target_fname': '{target_filename}.csv'
            }

filelist = os.listdir(file_loc['src_dir'])
for item in filelist:
    if item.endswith('.xlsm'):
        src_file = item
    else:
        continue
print('source file: ', src_file)

# this is the list of the fixed sheets
notincluded = pd.Series(['Instruction', 'Overview', 'Competitors', 'News', 'Revenue breakdown', 'Glassdoor Rating', 'PerformanceMetric',
                         'Scorecard', 'Analyst_Rating', '_CIQHiddenCacheSheet', 'TRS_Summary', 'TRS_decomp', 'Forecasts', 'Currencies'])

# this will read the excel file and create series to see all sheets
readexcel = pd.ExcelFile(src_file)
excelsheets = readexcel.sheet_names
excelsheets = pd.Series(excelsheets)

# this will get only the sheet for companies
percompanysheets = pd.concat([excelsheets, notincluded]).drop_duplicates(keep=False)

# this will get the information needed for the currency and other possible details
sheettoprocess = pd.DataFrame()
for sheets in percompanysheets:
    sheettoprocess = sheettoprocess.append(
        pd.read_excel(src_file, sheets, index=False, header=None, skiprows=2, usecols='C', nrows=10).T)

# this will create header for the company table
compheaders = pd.read_excel(src_file, sheets, index=False, skiprows=2, usecols='B', nrows=10, header=None).T
compheaders = compheaders.head(1)

# This will create the company table
df1 = compheaders.append([sheettoprocess])

df2 = df1
df2.columns = df2.iloc[0]
df2 = df2.drop(df2.iloc[0].index.name)
df2.columns.name = None
df2 = df2.rename(columns={' Company Name : ' : 'lineCompanyName'})
df2 = df2.rename(columns={' Currency : ' : 'sourceCurrency'})
companytable_final = df2[['lineCompanyName','sourceCurrency']]
companytable_final['requestID'] = src_file
companytable_final['requestID'] = companytable_final['requestID'].replace(".xlsm", "", regex=True)
#this can be added as reusable code
companytable_final['targetCurrency'] = companytable_final.loc[companytable_final['lineCompanyName'] == companytable_final['lineCompanyName'],'sourceCurrency'].iloc[0]
companytable_final.reset_index(drop=True, inplace=True)

#process data for 5 year trs
logging.info('replicating dataframes')
TRS_5yrs = pd.read_excel(src_file,sheet_name = 'TRS_Summary', skiprows = 44, index_cols = None, nrows = 41, usecols='B:BL')
TRS_5yrs.columns = range(TRS_5yrs.shape[1])
TRS_5yrs = TRS_5yrs.drop([1], axis=1)
TRS_5yrs = TRS_5yrs[TRS_5yrs[0].notna()]
TRS_5yrs = TRS_5yrs.transpose()
TRS_5yrs = TRS_5yrs[TRS_5yrs[0].notna()]

trsdatatoappend = pd.DataFrame()

for x_col in range(1, len(TRS_5yrs.columns)):
    line_trs_year = TRS_5yrs.iloc[0, 0] #get line trs year
    line_company_name = TRS_5yrs.iloc[0, x_col] #get line company name
    columnsData = TRS_5yrs.loc[ : ,[0, x_col]] #get the needed data using loop
    columnsData.columns =['lineTRSDateKey', 'lineTRSValue']
    columnsData = columnsData.drop([0])
    columnsData['lineCompanyName'] = line_company_name
    columnsData['lineTRSYear'] = line_trs_year
    trsdatatoappend = trsdatatoappend.append(columnsData)

#process data for 3 year trs
logging.info('replicating dataframes')
TRS_3yrs = pd.read_excel(src_file,sheet_name = 'TRS_Summary', skiprows = 87, index_cols = None, nrows = 41, usecols='B:BL')
TRS_3yrs.columns = range(TRS_3yrs.shape[1])
TRS_3yrs = TRS_3yrs.drop([1], axis=1)
TRS_3yrs = TRS_3yrs[TRS_3yrs[0].notna()]
TRS_3yrs = TRS_3yrs.transpose()
TRS_3yrs = TRS_3yrs[TRS_3yrs[0].notna()]

for x_col in range(1, len(TRS_3yrs.columns)):
    line_trs_year = TRS_3yrs.iloc[0, 0] #get line trs year
    line_company_name = TRS_3yrs.iloc[0, x_col] #get line company name
    columnsData = TRS_3yrs.loc[ : ,[0, x_col]] #get the needed data using loop
    columnsData.columns =['lineTRSDateKey', 'lineTRSValue']
    columnsData = columnsData.drop([0])
    columnsData['lineCompanyName'] = line_company_name
    columnsData['lineTRSYear'] = line_trs_year
    trsdatatoappend = trsdatatoappend.append(columnsData)

#process data for 1 year trs
logging.info('replicating dataframes')
TRS_1yrs = pd.read_excel(src_file,sheet_name = 'TRS_Summary', skiprows = 130, index_cols = None, nrows = 41, usecols='B:BL')
TRS_1yrs.columns = range(TRS_1yrs.shape[1])
TRS_1yrs = TRS_1yrs.drop([1], axis=1)
TRS_1yrs = TRS_1yrs[TRS_1yrs[0].notna()]
TRS_1yrs = TRS_1yrs.transpose()
TRS_1yrs = TRS_1yrs[TRS_1yrs[0].notna()]

for x_col in range(1, len(TRS_1yrs.columns)):
    line_trs_year = TRS_1yrs.iloc[0, 0] #get line trs year
    line_company_name = TRS_1yrs.iloc[0, x_col] #get line company name
    columnsData = TRS_1yrs.loc[ : ,[0, x_col]] #get the needed data using loop
    columnsData.columns =['lineTRSDateKey', 'lineTRSValue']
    columnsData = columnsData.drop([0])
    columnsData['lineCompanyName'] = line_company_name
    columnsData['lineTRSYear'] = line_trs_year
    trsdatatoappend = trsdatatoappend.append(columnsData)

trsdatatoappend.reset_index(drop=True, inplace=True)

#to merge the data and finalize the data
trs_line_chart_final = pd.merge(trsdatatoappend, companytable_final, on='lineCompanyName', how='left')

arrgCol = ['requestID','lineCompanyName','sourceCurrency','targetCurrency','lineTRSDateKey','lineTRSYear','lineTRSValue']
trs_line_chart_final = trs_line_chart_final[arrgCol]

trs_line_chart_final['lineTRSDateKey'] = pd.to_datetime(trs_line_chart_final['lineTRSDateKey'])
trs_line_chart_final['lineTRSDateKey'] = trs_line_chart_final['lineTRSDateKey'].apply(lambda x: x.strftime('%m/%d/%Y'))
trs_line_chart_final = trs_line_chart_final.set_index('requestID')

logging.info('Creating CSV file')
trs_line_chart_final.to_csv(os.path.join(file_loc['target_dir'], file_loc['target_fname']), header=True, encoding='utf-8-sig', date_format="%Y-%m-%d", index=True)
logging.info('CSV file done')
