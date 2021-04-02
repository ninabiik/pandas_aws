import pandas as pd
import os
import csv
import xlrd
import logging

file_loc = {'src_dir': r'{source_directory}',
            'target_dir': r'{target_directory}',
            'target_fname': '{target_fname}'
            }

filelist = os.listdir(file_loc['src_dir'])
for item in filelist:
    if item.endswith('.xlsm'):
        src_file = item
    else:
        continue
print('source file: ', src_file)

logging.basicConfig(level=logging.INFO)
logging.info('Reading excel file')

excel_xls = pd.ExcelFile(src_file)
TRS_Decomp = pd.read_excel(excel_xls, 'TRS_decomp', skiprows = 3, index_cols = 0, nrows = 40, usecols='B:BH')
print(TRS_Decomp)

logging.info('Getting the needed column, performed calculations and arrange')
#Needed columns
columns = ['Company Name', 'Revenue growth','Change in EBIT Margin',
           'Change in Multiple (EV/EBIT)','Dividend Yield','TRS Actual',
           'Capital structure impact','Starting Period','Latest Period']
TRS_Decomp1 = TRS_Decomp[columns]

#Calculations
TRS_Decomp2 = TRS_Decomp1[TRS_Decomp1['Company Name'].notna()]
TRS_Decomp2['Time_Period'] = '[' + TRS_Decomp2['Starting Period'].apply(lambda x: x.strftime('%m/%Y')) \
                           + ' - ' + TRS_Decomp2['Latest Period'].apply(lambda x: x.strftime('%m/%Y')) + ']'

#Arrange Columns
TRS_Decomp3 = TRS_Decomp2.rename(columns = {'Company Name':'CompanyName', 'Revenue growth':'Revenue_Growth',
                                            'Change in EBIT Margin':'Change_in_EBIT_Margin', 'Change in Multiple (EV/EBIT)':'Change_in_Multiple_EV_EBIT', 
                                            'Dividend Yield':'Dividend_Yield', 'TRS Actual':'TRS_Actual', 
                                            'Capital structure impact':'Capital_Structure_Impact'})
                                            
TRS_Decomp4 = TRS_Decomp3.drop(['Starting Period', 'Latest Period'], axis=1)
print(TRS_Decomp4)

logging.info('Generate CSV File TRS_Decomp')
#CSV File Creation
TRS_Decomp4.to_csv(os.path.join(file_loc['target_dir'], file_loc['target_fname']), encoding='utf-8-sig', date_format="%Y-%m-%d", index=False)
