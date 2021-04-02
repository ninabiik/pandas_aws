import json
import os 
import boto3 #this is needed to connect with other AWS services
import pandas as pd #this is used manipulate data. this is added on the lambda layer
import re
import sys #this is for checking of system
import s3fs #this is to access/move file to another s3 bucket. this is added on the lambda layer
import fsspec #this is to access/move file to another s3 bucket. this is added on the lambda layer
import csv #this is to read/export CSV
from io import BytesIO #this is for the lambda to read io from the file to be processed. this is added on the lambda layer
import logging #this is to know which step is currently process
#from pprint import pprint
#import datetime
from utilities import *
import datetime


def TRSBarchart_ETL(s3folder,s3filename):
    processbucket = os.environ['{bucketname}']
    
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart processing for ' + s3folder + ':'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart processing started'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart ReadFromS3 started'
    print(logmessage) 
	
    key = s3folder
    bucket = os.environ['{bucketname}']
    
    #this will read the file from S3
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    filedata = obj['Body'].read()
    df = pd.read_excel(BytesIO(filedata), engine = 'xlrd', sheet_name= 'TRS_Summary', skiprows = 2, index_cols = None, nrows = 40, usecols='B:O')
	
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart ReadFromS3 completed'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart ETL started'
    print(logmessage)
    
    #this is the list of the fixed sheets
    notincluded = pd.Series(['Overview','Competitors', 'Instruction','Glassdoor Rating', 'PerformanceMetric', 'Scorecard', 'Analyst_Rating','_CIQHiddenCacheSheet', 'TRS_Summary', 'TRS_decomp', 'TRS_Covid', 'Forecasts', 'Currencies','Segment Benchmarking (Business)','Segment Benchmarking (Geo.)'])
    
    #this will read the excel file and create series to see all sheets
    readexcel = pd.ExcelFile(BytesIO(filedata), engine = 'xlrd')
    excelsheets = readexcel.sheet_names
    excelsheets = pd.Series(excelsheets)
    
    #this will get only the sheet for companies
    percompanysheets = pd.concat([excelsheets,notincluded]).drop_duplicates(keep=False)
    
    #this will get the information needed for the currency and other possible details
    #sheettoprocess = pd.DataFrame()
    #for sheets in percompanysheets:
    #    sheettoprocess = sheettoprocess.append(pd.read_excel(BytesIO(filedata), engine='xlrd', sheet_name=sheets, index=False, header=None, skiprows=2, usecols='D', nrows=4).T)
    
    #this will get the information needed for the currency and other possible details
    sheettoprocess = pd.DataFrame()
    companyName = pd.DataFrame()
    # sheettoprocess.insert(0, 'lkup_companyname', "")
    for sheets in percompanysheets:
        # sheettoprocess["lkup_companyname"] = sheets
        sheettoprocess = sheettoprocess.append(pd.read_excel(BytesIO(filedata), sheet_name=sheets, engine='xlrd', index=False, header=None, skiprows=2, usecols='D', nrows=4).T)
        sheetName = pd.Series(sheets)
        companyName['lkup_companyname'] = sheetName
        sheettoprocess = sheettoprocess.append(companyName)
        
    sheettoprocess.reset_index(drop=True, inplace=True)
    sheettoprocess['lkup_companyname'] = sheettoprocess['lkup_companyname'].shift(-1)
    sheettoprocess = sheettoprocess[sheettoprocess[0].notna()]
    
    #this will create header for the company table
    compheaders = pd.read_excel(BytesIO(filedata), engine='xlrd', sheet_name=sheets, index=False, skiprows=2, usecols='B', nrows=4, header=None).T
    compheaders = compheaders.head(1)
    
    #This will create the company table
    companytable = compheaders.append([sheettoprocess])
    
    #this will clean the index created by the transpose
    #this will be the reusable table
    companytable.columns = companytable.iloc[0]
    companytable = companytable.drop(companytable.iloc[0].index.name)
    companytable.columns.name = None

    companytable = companytable.rename(columns={' Company Name : ' : 'CompanySheetName'})
    companytable.columns.values[4] = "CompanyName"
    
    companytable = companytable[companytable['CompanyName'].notna()]
    
    #this will process the data for the barchart
    logging.info('replicating dataframes')
    TRS_Summary1 = df
    TRS_Summary3 = df
    TRS_Summary5 = df
    
    logging.info('Getting the TRS Year 5')
    columns = ['Company', ' TRS']
    TRS_Summary5 = TRS_Summary5[columns]
    TRS_Summary5 = TRS_Summary5[TRS_Summary5['Company'].notna()]
    TRS_Summary5 = TRS_Summary5.rename(columns = {' TRS': 'TRS'})
    TRS_Summary5.insert(2, 'TRS_Year', 5)
    
    logging.info('Getting the TRS Year 3')
    columns = ['Company', ' TRS.1']
    TRS_Summary3 = TRS_Summary3[columns]
    TRS_Summary3 = TRS_Summary3[TRS_Summary3['Company'].notna()]
    TRS_Summary3 = TRS_Summary3.rename(columns = {' TRS.1': 'TRS'})
    TRS_Summary3.insert(2, 'TRS_Year', 3)
    
    logging.info('Getting the TRS Year 1')
    columns = ['Company', ' TRS.2']
    TRS_Summary1 = TRS_Summary1[columns]
    TRS_Summary1 = TRS_Summary1[TRS_Summary1['Company'].notna()]
    TRS_Summary1 = TRS_Summary1.rename(columns = {' TRS.2': 'TRS'})
    TRS_Summary1.insert(2, 'TRS_Year', 1)
    
    logging.info('Appending the TRS_Summary Tables')
    TRS_Barchart_Data = TRS_Summary1.append([TRS_Summary3,TRS_Summary5])
    TRS_Barchart_Data = TRS_Barchart_Data.rename(columns={'Company' : 'CompanyName'})
    
    TRS_Barchart_Data.insert(0,'requestID',key)
    TRS_Barchart_Data['requestID'] = TRS_Barchart_Data['requestID'].apply(lambda x:x.split("/")[2])
    TRS_Barchart_Data['requestID'] = TRS_Barchart_Data['requestID'].apply(lambda x:x.split(".")[0])
    
    #this line can be added to created into a reusable code.
    TRS_Barchart_Data['TargetCompany'] = TRS_Barchart_Data['requestID'].apply(lambda x:x.split("_")[0])
    
    #this will create the final table
    TRS_Barchart_Final = pd.merge(TRS_Barchart_Data, companytable, on = ['CompanyName', 'CompanyName'])
    columns = ['TargetCompany', 'CompanyName', 'requestID' , 'TRS', 'TRS_Year', 'Company Ticker:', ' Currency : ']
    TRS_Barchart_Final = TRS_Barchart_Final[columns]
    TRS_Barchart_Final = TRS_Barchart_Final.rename(columns={'TRS' : 'TRS_Value', 'Company Ticker:' : 'Company_Ticker', ' Currency : ' : 'Source_Currency'})
    #this can be added as reusable code
    TRS_Barchart_Final['TargetCurrency'] = TRS_Barchart_Final.loc[TRS_Barchart_Final['TargetCompany'] == TRS_Barchart_Final['TargetCompany'],'Source_Currency'].iloc[0]
    
    excelname = s3filename
    
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart ETL completed'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart putToS3 started'
    print(logmessage)
    
    time = int(generateEpochTime(datetime.datetime.now()))
    filename = s3filename
    finaltable = TRS_Barchart_Final.to_json(orient='records')
    dashboard = 'trsbarchart' + s3filename.split('_')[0]
    id = s3folder.split('/')[1]
    insertID = 'custom#' + id + '#' + s3filename
    key = s3filename + '#trsbarchart'
    
    try:
        s3 = s3fs.S3FileSystem()
    except Exception as e:
        logging.exception("Exception occurred")
    fileName = "trsbarchart.csv"
    destination = processbucket+id+"/"+excelname+"/"+fileName
    TRS_Barchart_Final.to_csv(destination)
	
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart putToS3 completed'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart loadToDynamoDB started'
    print(logmessage)
    
    requestid_resp = loadToDynamoDB(insertID,
                                    key,
                                    finaltable,
                                    dashboard,
                                    datetime.datetime.today().strftime('%Y-%m-%d'),
                                    str(datetime.datetime.now()))
	
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart loadToDynamoDB completed'
    print(logmessage)
    logmessage = str(datetime.datetime.now()) + '\tTRSBarchart processing completed'
    print(logmessage)
        
    return {
        'statusCode': 200,
        'body': json.dumps('TRSBarchart Processing Completed')
    }
