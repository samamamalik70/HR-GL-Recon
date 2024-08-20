import pandas as pd
import os
import openpyxl as xl
from configparser import ConfigParser
import smtplib

class FilteringData:

    def read_config(self, path):
        """this function returns path of the directory which contains the GL Recon File, 
           param: path   ---> it takes the config file path as parameter
        """
        if path is not None:
            config = ConfigParser()
            config.read(path)
            dir_path = config.get('PATHS', 'gl_path')
            if os.path.exists(dir_path):
                return dir_path
            else:
                return None
        else:
            return None

    def read_excel_sheets(self, path1, path2):
        if [path1, path2] is not None:
            statement_df = pd.read_excel(path1)
            statement_df['Trx Type'] = ''
            statement_df['Status'] = ''
            statement_df['Remarks'] = ''
            moneta_df = pd.read_excel(path2)
            return statement_df, moneta_df
        else:
            return None

    def merge_dataframes(self, df1, df2):
        '''Merges two dataframes one is Moneta Report sheet and second is the statement report sheet
        '''
        if [df1, df2] is not None:
            df1['Trx Type'].fillna('', inplace=True)
            df1['Status'].fillna('', inplace=True)
            df1['Remarks'].fillna('', inplace=True)
            df1['CRN'].fillna('', inplace=True)
            merged_df = df1.merge(df2, left_on="CRN", right_on="Control Reference Number", how='left',
                                 suffixes=('', '_df'),indicator=True)

            merged_df = merged_df[(merged_df['_merge'] == 'both') | (merged_df['_merge'] == 'left_only')]
            transaction= merged_df['Transaction Type']
            status = merged_df['Status_df']
            df1['Trx Type'] = merged_df['Transaction Type']
            df1['Status']= merged_df['Status_df']
            return df1, df2
        else:
            return None

    def apply_filters_on_df(self, output_path, df1, df2):
        ''' this applies  filters on the updates remarks column of statement report according to the filter rules applied this 
        takes statement report dataframe as df1, moneta report dataframe as df2 and gl_file path which is output path al last it 
        if filters successfully applied it returns True otherwise it returns False 
        parameters: df1 
        parameters: df2
        parameters: output_path 
        '''
        if [output_path, df1, df2] is not None:
            df1['CRN'].fillna('', inplace=True)
            values = ['wallet balanced', 'Wallet auto reversed']
            bcio_filter = ((df1['CRN'] == '') & (df1['GL Name'].str.contains('BCIO')))
            df1.loc[bcio_filter, 'Remarks'] = 'BCIO and IR adjustments'
            wallet_exp_filter = ((df1['Status'].isin(['Successful', 'AML Suspected', 'Imported']))
                                    & (df1['Trx Type'] == 'C-MA') & (df1['Count'].isin([1, 2, 3, 5])))
            df1.loc[wallet_exp_filter, 'Remarks'] = 'Wallet exception'
            ibft_filter = ((df1['Status'].isin(['ThirdPartyExported', 'AML Suspected'])) &
                              (df1['Trx Type'] == 'CASH TO OTHER ACCOUNT') & (df1['Count'] == 2))
            df1.loc[ibft_filter, 'Remarks'] = 'IBFT exception'
            wallet_balance_filter = ((df1['Trx Type'].str.contains('C-MA')) & (df1['Status'].str.contains('Successful')) & (
                    df1['Count'] == 7))
            df1.loc[wallet_balance_filter, 'Remarks'] = 'wallet balanced'
            ibft_balance_filter = ((df1['Status'] == 'ThirdPartyExported') & (df1['Trx Type'] == 'CASH TO OTHER ACCOUNT') & (
                    df1['Count'] == 4))
            df1.loc[ibft_balance_filter, 'Remarks'] = 'IBFT balanced'
            cleanse_failed_filter = ((df1['Trx Type'] == 'C-MA') & (df1['Status'] == 'Cleanse Failed'))
            df1.loc[cleanse_failed_filter, 'Remarks'] = 'Wallet auto reversed'
            df1.sort_values(by='CRN', inplace=True)
            hr_payable_debit = ((df1['Status'] == 'Successful') & (df1['Trx Type'] == 'C-MA') & (df1['Count'] == 11) &
                           (df1['GL DR/CR'] == 'HR Payable DEBIT'))
            df1.loc[hr_payable_debit, 'Remarks'] = values * (hr_payable_debit.sum() // len(values)) + values[
                                                                                                          :hr_payable_debit.sum() % len(
                                                                                                              values)]
            hr_payable_credit = ((df1['Status'] == 'Successful') & (df1['Trx Type'] == 'C-MA') & (df1['Count'] == 11) &
                           (df1['GL DR/CR'] == 'HR Payable CREDIT'))
            df1.loc[hr_payable_credit, 'Remarks'] = values * (hr_payable_credit.sum() // len(values)) + values[
                                                                                                          :hr_payable_credit.sum() % len(
                                                                                                              values)]
            partner_debit = ((df1['Status'] == 'Successful') & (df1['Trx Type'] == 'C-MA') & (df1['Count'] == 11) &
                           (df1['GL DR/CR'] == 'Partners DEBIT'))
            df1.loc[partner_debit, 'Remarks'] = values * (partner_debit.sum() // len(values)) + values[
                                                                                                          :partner_debit.sum() % len(
                                                                                                              values)]
            partner_credit = ((df1['Status'] == 'Successful') & (df1['Trx Type'] == 'C-MA') & (df1['Count'] == 11) &
                          (df1['GL DR/CR'] =='Partners CREDIT'))
            df1.loc[partner_credit, 'Remarks'] = 'Wallet auto reversed'
            wallet_balance = ((df1['Status'] == 'Successful') & (df1['Trx Type'] == 'C-MA') & (df1['Count'] == 11) &
                          (~df1['Remarks'].isin(values)))
            df1.loc[wallet_balance, 'Remarks'] = 'wallet balanced'
            prefund_filter = ((df1['Trx Type'].isnull()) & (df1['Status'].isnull()) & (df1['Remarks'] == '')
                              & (df1['CRN'] == '') | (df1['CRN'] == 'Funds Transfer from INC to PWP'))
            df1.loc[prefund_filter, 'Remarks'] = 'Partner prefund'
            df1.to_excel('res.xlsx', index=False)
            with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df1.to_excel(writer, sheet_name= 'Data 07-08-2024', index=False)
                df2.to_excel(writer, sheet_name='Moneta Report', index=False)
            return True
        else:
            return None

    def update_summary(self, file_path):
        if file_path is not None:
            statement_df = pd.read_excel(file_path, sheet_name='Data 07-08-2024')
            homesend_filter = statement_df[(statement_df['GL Name'] == 'HomeSend') & (statement_df['Remarks'] == 'Partner prefund')]
            homesend_credit_partner = homesend_filter['Credit'].sum()
            homesend_debit_partner = -(homesend_filter['Debit'].sum())
            homesend_filter = statement_df[((statement_df['GL Name'] == 'HomeSend')) & (statement_df['Remarks'] == 'wallet balanced')]
            homesend_debit_wb = -(homesend_filter['Debit'].sum())
            homesend_credit_wb = (homesend_filter['Credit'].sum())
            homesend_filter = statement_df[((statement_df['GL Name'] == 'HomeSend')) & (statement_df['Remarks'] == 'Wallet exception')]
            homesend_credit_we = (homesend_filter['Credit'].sum())
            homesend_debit_we = -(homesend_filter['Debit'].sum())
            homesend_filter = statement_df[((statement_df['GL Name'] == 'HomeSend')) & (statement_df['Remarks'] == 'Wallet auto reversed')]
            homesend_credit_wa = (homesend_filter['Credit'].sum())
            homesend_debit_wa = -(homesend_filter['Debit'].sum())
            payoneer_wallet = statement_df[((statement_df['GL Name'] == 'Payoneer INC - MWallet'))
                                           & (statement_df['Remarks'] == 'Partner prefund')]
            payoneer_wallet_pp_credit = payoneer_wallet['Credit'].sum()
            payoneer_wallet_pp_debit = -(payoneer_wallet['Debit'].sum())
            payoneer_wallet = statement_df[((statement_df['GL Name'] == 'Payoneer INC - MWallet')) & (statement_df['Remarks'] == 'wallet balanced')]
            payoneer_wallet_wb_credit = payoneer_wallet['Credit'].sum()
            payoneer_wallet_wb_debit = -(payoneer_wallet['Debit'].sum())
            payoneer_wallet = statement_df[((statement_df['GL Name'] == 'Payoneer INC - MWallet')) & (statement_df['Remarks'] =='Wallet exception')]
            payoneer_wallet_we_debit = -(payoneer_wallet['Debit'].sum())
            payoneer_wallet_we_credit = payoneer_wallet['Credit'].sum()
            payoneer_wallet = statement_df[((statement_df['GL Name'] == 'Payoneer INC - MWallet')) & (statement_df['Remarks'] =='Wallet auto reversed')]
            payoneer_wallet_wa_debit = -(payoneer_wallet['Debit'].sum())
            payoneer_wallet_wa_credit = payoneer_wallet['Credit'].sum()
            payoneer_inc = statement_df[((statement_df['GL Name'] == 'Payoneer Inc.')) & (statement_df['Remarks'] == 'Partner prefund')]
            payoneer_inc_pp_credit = payoneer_inc['Credit'].sum()
            payoneer_inc_pp_debit = -(payoneer_inc['Debit'].sum())
            payoneer_inc = statement_df[((statement_df['GL Name'] == 'Payoneer Inc.')) & (statement_df['Remarks'] == 'IBFT balanced')]
            payoneer_inc_ibft_credit = payoneer_inc['Credit'].sum()
            payoneer_inc_ibft_debit = -(payoneer_inc['Debit'].sum())
            payoneer_inc = statement_df[((statement_df['GL Name'] == 'Payoneer Inc.')) & (statement_df['Remarks'] == 'IBFT exception')]
            payoneer_inc_ibft_exp_credit = payoneer_inc['Credit'].sum()
            payoneer_inc_ibft_exp_debit = -(payoneer_inc['Debit'].sum())
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable CREDIT')) & (statement_df['Remarks'] == 'wallet balanced')]
            hr_payable_wb_credit = hr_payable_filter['Credit'].sum()
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable CREDIT')) & (statement_df['Remarks'] == 'Wallet exception')]
            hr_payable_we_credit = hr_payable_filter['Credit'].sum()

            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable CREDIT')) & (statement_df['Remarks'] == 'Wallet auto reversed')]
            hr_payable_wa_credit = hr_payable_filter['Credit'].sum()

            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable CREDIT')) & (statement_df['Remarks'] == 'IBFT balanced')]
            hr_payable_ibft_balance_credit = hr_payable_filter['Credit'].sum()
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable CREDIT')) & (statement_df['Remarks'] == 'IBFT exception')]
            hr_payable_ibft_exp_credit = hr_payable_filter['Credit'].sum()
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable DEBIT')) & (statement_df['Remarks'] == 'wallet balanced')]
            hr_payable_wb_debit = -(hr_payable_filter['Debit'].sum())
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable DEBIT')) & (statement_df['Remarks'] == 'Wallet exception')]
            hr_payable_we_debit = -(hr_payable_filter['Debit'].sum())
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable DEBIT')) & (statement_df['Remarks'] == 'Wallet auto reversed')]
            hr_payable_wa_debit = -(hr_payable_filter['Debit'].sum())
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable DEBIT')) & (statement_df['Remarks'] == 'IBFT balanced')]
            hr_payable_ibft_balance_debit = -(hr_payable_filter['Debit'].sum())
            hr_payable_filter = statement_df[((statement_df['GL DR/CR'] == 'HR Payable DEBIT')) & (statement_df['Remarks'] == 'IBFT exception')]
            hr_payable_ibft_exp_debit = -(hr_payable_filter['Debit'].sum())
            hr_settlement_filter = statement_df[((statement_df['GL Name'] == 'HR Settlement')) & (statement_df['Remarks'] == 'wallet balanced')]
            hr_set_wb_credit = hr_settlement_filter['Credit'].sum()
            hr_set_wb_debit = -(hr_settlement_filter['Debit'].sum())
            hr_settlement_filter = statement_df[((statement_df['GL Name'] == 'HR Settlement')) & (statement_df['Remarks'] == 'Wallet exception')]
            hr_set_we_credit = hr_settlement_filter['Credit'].sum()
            hr_set_we_debit = -(hr_settlement_filter['Debit'].sum())
            bcio_filter = statement_df[((statement_df['GL Name'] == 'BCIO')) & (statement_df['Remarks'] == 'BCIO and IR adjustments')]
            bcio_credit_sum = bcio_filter['Credit'].sum()
            bcio_debit_sum = -(bcio_filter['Debit'].sum())
            bcio_filter = statement_df[((statement_df['GL Name'] == 'BCIO')) & (statement_df['Remarks'] == 'wallet balanced')]
            bcio_wb_credit = (bcio_filter['Credit'].sum())
            bcio_wb_debit = -(bcio_filter['Debit'].sum())
            bcio_filter = statement_df[((statement_df['GL Name'] == 'BCIO')) & (statement_df['Remarks'] == 'Wallet exception')]
            bcio_we_credit = bcio_filter['Credit'].sum()
            bcio_we_debit = -(bcio_filter['Debit'].sum())
            ir_ledger  = statement_df[((statement_df['GL Name'] == 'IR Ledger')) & (statement_df['Remarks'] == 'wallet balanced')]
            # this will be the sum of the debit and credit entries for ir ledger wallet balance case 
            ir_ledger_credit = (ir_ledger['Credit'].sum())
            ir_ledger_debit = -(ir_ledger['Debit'].sum())
            ir_ledger = statement_df[((statement_df['GL Name'] == 'IR Ledger')) & (statement_df['Remarks'] == 'Wallet exception')]
            # this will be the sum of debit and credit for wallet exception 
            ir_ledger_we_credit = ir_ledger['Credit'].sum()
            ir_ledger_we_debit = -(ir_ledger['Debit'].sum())
            one_link_filter = statement_df[((statement_df['GL Name'] == '1 Link Settlement')) & (statement_df['Remarks'] == 'IBFT balanced')]
            one_link_balance_credit = one_link_filter['Credit'].sum()
            one_link_balance_debit = -(one_link_filter['Debit'].sum())
            one_link_filter = statement_df[((statement_df['GL Name'] == '1 Link Settlement')) & (statement_df['Remarks'] == 'IBFT exception')]
            one_link_exception_credit = one_link_filter['Credit'].sum()
            one_link_exception_debit = -(one_link_filter['Debit'].sum())
            # update the summary sheet according to filters applied on the GL Name Column and the Remarks Column.

            req_wb = xl.load_workbook(file_path)
            summary_sheet = req_wb['Summary']
            summary_sheet['B5'] = homesend_credit_partner
            summary_sheet['C5'] = homesend_debit_partner
            summary_sheet['B7'] = homesend_credit_wb
            summary_sheet['C7'] = homesend_debit_wb
            summary_sheet['B8'] = homesend_credit_we
            summary_sheet['C8'] = homesend_debit_we
            summary_sheet['B9'] = homesend_credit_wa
            summary_sheet['C9'] = homesend_debit_wa
            summary_sheet['F5'] = payoneer_wallet_pp_credit
            summary_sheet['G5'] = payoneer_wallet_pp_debit
            summary_sheet['F7'] = payoneer_wallet_wb_credit
            summary_sheet['G7'] = payoneer_wallet_wb_debit
            summary_sheet['G8'] = payoneer_wallet_we_debit
            summary_sheet['F8'] = payoneer_wallet_we_credit
            summary_sheet['F9'] = payoneer_wallet_wa_credit
            summary_sheet['G9'] = payoneer_wallet_wa_debit
            summary_sheet['H5'] = payoneer_inc_pp_credit
            summary_sheet['I5'] = payoneer_inc_pp_debit
            summary_sheet['H10'] = payoneer_inc_ibft_credit
            summary_sheet['I10'] = payoneer_inc_ibft_debit
            summary_sheet['H11'] = payoneer_inc_ibft_exp_credit
            summary_sheet['I11'] = payoneer_inc_ibft_exp_debit
            summary_sheet['C22'] = hr_payable_wb_credit
            summary_sheet['D22'] = hr_payable_wb_debit
            summary_sheet['C23'] = hr_payable_we_credit
            summary_sheet['D23'] = hr_payable_we_debit
            summary_sheet['C24'] = hr_payable_wa_credit
            summary_sheet['D24'] = hr_payable_wa_debit
            summary_sheet['C25'] = hr_payable_ibft_balance_credit
            summary_sheet['D25'] = hr_payable_ibft_balance_debit
            summary_sheet['C26'] = hr_payable_ibft_exp_credit
            summary_sheet['D26'] = hr_payable_ibft_exp_debit
            summary_sheet['E22'] = hr_set_wb_credit
            summary_sheet['F22'] = hr_set_wb_debit
            summary_sheet['E23'] = hr_set_we_credit
            summary_sheet['F23'] = hr_set_we_debit
            summary_sheet['G21'] = bcio_credit_sum
            summary_sheet['H21'] = bcio_debit_sum
            summary_sheet['G22'] = bcio_wb_credit
            summary_sheet['H22'] = bcio_wb_debit
            summary_sheet['G23'] = bcio_we_credit
            summary_sheet['H23'] = bcio_we_debit
            summary_sheet['I22'] = ir_ledger_credit
            summary_sheet['J22'] = ir_ledger_debit
            summary_sheet['I23'] = ir_ledger_we_credit
            summary_sheet['J23'] = ir_ledger_we_debit
            summary_sheet['K25'] = one_link_balance_credit
            summary_sheet['L25'] = one_link_balance_debit
            summary_sheet['K26'] = one_link_exception_credit
            summary_sheet['L26'] = one_link_exception_debit
            req_wb.save(file_path)
            return True
        else:
            return False
    
    
    def send_notification(self):
        """this function send email to the end user of this process whatever the status is
           either failure or Successful
        """
        SERVER = "smtp.outlook.com"
        PORT = 587
        FROM = "samama.tariq@mercurialminds.com"
        TO = ["samama.tariq@mercurialminds.com"]
        SUBJECT = "Process successful"
        TEXT = ("All Data in the Statement Report file and Moneta file is handled correctly, "
                "remarks added and all GL And ALL partner entries are done successfully.")
        message = f"""From: {FROM}\r\nTo: {", ".join(TO)}\r\nSubject: {SUBJECT}\r\n\r\n{TEXT}"""
        USERNAME = 'samama.tariq@mercurialminds.com'
        PASSWORD = 'Anber@2006!'
        try:
            server = smtplib.SMTP(SERVER, PORT)
            server.starttls()
            server.login(USERNAME, PASSWORD)
            server.sendmail(FROM, TO, message)
            print("Email sent successfully")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            server.quit() 


