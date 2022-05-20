import pandas as pd
import sqlalchemy
from functools import reduce
import os
import numpy as np
import hidden

# Copy & paste calendarization function but change the and conditionals to and
#  when writing the fiscal_period also make sure to adjust fand number of qrts
#  because some companies shoq q4 results on 10-k
# drop duplicates where ticker, tag, year, fiscal period, adsh, qtrs, num_value, crdr
# all match since some lines have different plabels/line sequences

engine = sqlalchemy.create_engine(hidden.db_route)
df = pd.read_sql_table('findatanotes_example', engine)

df.drop_duplicates(subset=['ticker', 'stmt', 'tag', 'year', 'fiscal_period', 'adsh', 'qtrs', 'num_value', 'crdr'])

df = df[df.inpth != 1]
df['plabel'] = df['plabel'].str.lower()
df['plabel'] = df['plabel'].str.title()
df.loc[(df.tag == 'ComprehensiveIncomeNetOfTaxAttributableToNoncontrollingInterest'),
'crdr'] = 'C'
df.loc[(df.tag == 'ComprehensiveIncomeNetOfTax'),
'crdr'] = 'C'
df.loc[(df.tag == 'OtherComprehensiveIncomeLossPensionAndOtherPostretirementBenefitPlansAdjustmentNetOfTax'),
'crdr'] = 'C'
df.loc[(df.tag == 'AccumulatedOtherComprehensiveIncomeLossDefinedBenefitPensionAndOtherPostretirementPlansNetOfTax'),
'crdr'] = 'C'
df.loc[(df.tag == 'AccumulatedOtherComprehensiveIncomeLossTax'),
'crdr'] = 'C'

df.loc[df['tag'].isin(['EarningsPerShareDiluted', 'EarningsPerShareBasic', 'WeightedAverageNumberOfDilutedSharesOutstanding',
'WeightedAverageNumberOfSharesOutstandingBasic', 'EarningsPerShareBasicAndDiluted', 
'WeightedAverageNumberOfShareOutstandingBasicAndDiluted', 'CommonStockDividendsPerShareCashPaid', 
'WeightedAverageLimitedPartnershipUnitsOutstandingDiluted', 'WeightedAverageNumberOfLimitedPartnershipAndGeneralPartnershipUnitOutstandingBasicAndDiluted'
'CommonStockSharesOutstanding', 'WeightedAverageNumberOfSharesOutstandingBasicAndDiluted1']), 'crdr'] = 'O'


# All BS fslis that are assigned Q4 for fiscal_period will also act as FY values 
# sometimes month_day values are one month behind or ahead or the 3 month time span...
def add_fp(df):
    if df['qtrs'] == 1 or df['qtrs'] == 0:

        if df['fye'] == 131:
            if df['month_day'] == 430 or df['month_day'] == 531 or df['month_day'] == 331:
                return 'Q1'
            elif df['month_day'] == 731 or df['month_day'] == 630 or df['month_day'] == 831:
                return 'Q2'
            elif df['month_day'] == 1031 or df['month_day'] == 930 or df['month_day'] == 1130:
                return 'Q3'
            elif df['month_day'] == 131 or df['month_day'] == 1231 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q4'

        elif df['fye'] == 228 or df['fye'] == 229:
            if df['month_day'] == 531 or df['month_day'] == 630 or df['month_day'] == 430:
                return 'Q1'
            elif df['month_day'] == 831 or df['month_day'] == 930 or df['month_day'] == 1031:
                return 'Q2'
            elif df['month_day'] == 1130 or df['month_day'] == 1031 or df['month_day'] == 1231:
                return 'Q3'
            elif df['month_day'] == 228 or df['month_day'] == 229 or df['month_day'] == 131 or df['month_day'] == 331:
                return 'Q4'

        elif df['fye'] == 331:
            if df['month_day'] == 630 or df['month_day'] == 531 or df['month_day'] == 731:
                return 'Q1'
            elif df['month_day'] == 930 or df['month_day'] == 1031 or df['month_day'] == 831:
                return 'Q2'
            elif df['month_day'] == 1231 or df['month_day'] == 131 or df['month_day'] == 1130:
                return 'Q3'
            elif df['month_day'] == 331 or df['month_day'] == 430 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q4'

        if df['fye'] == 430:
            if df['month_day'] == 731 or df['month_day'] == 630 or df['month_day'] == 831:
                return 'Q1'
            elif df['month_day'] == 1031 or df['month_day'] == 930 or df['month_day'] == 1130:
                return 'Q2'
            elif df['month_day'] == 131 or df['month_day'] == 1231 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q3'
            elif df['month_day'] == 430 or df['month_day'] == 531 or df['month_day'] == 331:
                return 'Q4'

        elif df['fye'] == 531:
            if df['month_day'] == 831 or df['month_day'] == 930 or df['month_day'] == 1031:
                return 'Q1'
            elif df['month_day'] == 1130 or df['month_day'] == 1031 or df['month_day'] == 1231:
                return 'Q2'
            elif df['month_day'] == 228 or df['month_day'] == 229 or df['month_day'] == 131 or df['month_day'] == 331:
                return 'Q3'
            elif df['month_day'] == 531 or df['month_day'] == 630 or df['month_day'] == 430:
                return 'Q4'

        elif df['fye'] == 630:
            if df['month_day'] == 930 or df['month_day'] == 1031 or df['month_day'] == 831:
                return 'Q1'
            elif df['month_day'] == 1231 or df['month_day'] == 131 or df['month_day'] == 1130:
                return 'Q2'
            elif df['month_day'] == 331 or df['month_day'] == 430 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q3'
            elif df['month_day'] == 630 or df['fye'] == 531 or df['month_day'] == 731:
                return 'Q4'
        
        if df['fye'] == 731:
            if df['month_day'] == 1031 or df['month_day'] == 930 or df['month_day'] == 1130:
                return 'Q1'
            elif df['month_day'] == 131 or df['month_day'] == 1231 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q2'
            elif df['month_day'] == 430 or df['month_day'] == 531 or df['month_day'] == 331:
                return 'Q3'
            elif df['month_day'] == 731 or df['month_day'] == 630 or df['month_day'] == 831:
                return 'Q4'

        elif df['fye'] == 831:
            if df['month_day'] == 1130 or df['month_day'] == 1031 or df['month_day'] == 1231:
                return 'Q1'
            elif df['month_day'] == 228 or df['month_day'] == 229 or df['month_day'] == 131 or df['month_day'] == 331:
                return 'Q2'
            elif df['month_day'] == 531 or df['month_day'] == 630 or df['month_day'] == 430:
                return 'Q3'
            elif df['month_day'] == 831 or df['month_day'] == 930 or df['month_day'] == 1031:
                return 'Q4'

        elif df['fye'] == 930:
            if df['month_day'] == 1231 or df['month_day'] == 131 or df['month_day'] == 1130:
                return 'Q1'
            elif df['month_day'] == 331 or df['month_day'] == 430 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q2'
            elif df['month_day'] == 630 or df['month_day'] == 531 or df['month_day'] == 731:
                return 'Q3'
            elif df['month_day'] == 930 or df['month_day'] == 1031 or df['month_day'] == 831:
                return 'Q4'

        if df['fye'] == 1031:
            if df['month_day'] == 131 or df['month_day'] == 1231 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q1'
            elif df['month_day'] == 430 or df['month_day'] == 531 or df['month_day'] == 331:
                return 'Q2'
            elif df['month_day'] == 731 or df['month_day'] == 630 or df['month_day'] == 831:
                return 'Q3'
            elif df['month_day'] == 1031 or df['month_day'] == 930 or df['month_day'] == 1130:
                return 'Q4'

        elif df['fye'] == 1130:
            if df['month_day'] == 228 or df['month_day'] == 229 or df['month_day'] == 131 or df['month_day'] == 331:
                return 'Q1'
            elif df['month_day'] == 531 or df['month_day'] == 630 or df['month_day'] == 430:
                return 'Q2'
            elif df['month_day'] == 831 or df['month_day'] == 930 or df['month_day'] == 1031:
                return 'Q3'
            elif df['month_day'] == 1130  or df['month_day'] == 1031 or df['month_day'] == 1231:
                return 'Q4'
        
        elif df['fye'] == 1231:
            if df['month_day'] == 331 or df['month_day'] == 430 or df['month_day'] == 228 or df['month_day'] == 229:
                return 'Q1'
            elif df['month_day'] == 630 or df['month_day'] == 531 or df['month_day'] == 731:
                return 'Q2'
            elif df['month_day'] == 930 or df['month_day'] == 1031 or df['month_day'] == 831:
                return 'Q3'
            elif df['month_day'] == 1231 or df['month_day'] == 131 or df['month_day'] == 1130:
                return 'Q4'

    elif df['qtrs'] == 4:
        if df['fye'] == df['month_day'] or np.absolute(df['fye'] - df['month_day']) == 1:
            return 'FY'

IS_comm = df[df.stmt == 'IS']
BS_comm = df[df.stmt == 'BS']
CF_comm = df[df.stmt == 'CF']

listofStmts = [IS_comm, BS_comm, CF_comm]
df2 = pd.concat(listofStmts)
df2['fiscal_period'] = df2.apply(add_fp, axis=1)
df3 = df2[['ticker', 'fye', 'stmt', 'tag', 'crdr', 'line', 'plabel', 'qtrs', 'tlabel', 'num_value', 'year', 'month_day', 'fiscal_period', 'classification']]
df4 = df3.drop_duplicates(subset=['ticker', 'stmt', 'tag', 'year', 'fiscal_period', 'num_value', 'crdr'])

# Separate out based on stmt, debit or credit, or financial
df_financial = df4[df4.classification == 'financial']
IS_comm = df4[df4.stmt == 'IS']
BS_comm = df4[df4.stmt == 'BS']
CF_comm = df4[df4.stmt == 'CF']

IS_comm_credit = IS_comm[IS_comm.crdr == 'C']
IS_comm_Debit = IS_comm[IS_comm.crdr == 'D']
BS_comm_debit = BS_comm[BS_comm.crdr == 'D']
BS_comm_credit = BS_comm[BS_comm.crdr == 'C']

NoActg = df4[df4.crdr == 'O']

# Commercial credit IS changes
# NOTES: the tag 'Revenues' can be either net or gross revenue depending on ticker

#Revenue line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'RevenueFromContractWithCustomerExcludingAssessedTax'),
'fsli'] = 'Net Revenue'
IS_comm_credit.loc[(IS_comm_credit.tag == 'RevenueFromContractWithCustomerIncludingAssessedTax'),
'fsli'] = 'Net Revenue'
IS_comm_credit.loc[(IS_comm_credit.tag == 'SalesRevenueNet'),
'fsli'] = 'Net Revenue'
IS_comm_credit.loc[(IS_comm_credit.tag == 'Revenues'),
'fsli'] = 'Net Revenue'

# Gross profit line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'GrossProfit'),
'fsli'] = 'Gross Profit'

# Operating Income line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'OperatingIncomeLoss'),
'fsli'] = 'Operating Income'

# profit before tax line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'),
'fsli'] = 'Net Profit Before Taxes'
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments'),
'fsli'] = 'Net Profit Before Taxes'
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest'),
'fsli'] = 'Net Profit Before Taxes'

#net income line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'ProfitLoss'),
'fsli'] = 'Total Profit(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'NetIncomeLossAvailableToCommonStockholdersBasic'),
'fsli'] = 'Net Income'
IS_comm_credit.loc[(IS_comm_credit.tag == 'NetIncomeLoss'),
'fsli'] = 'Net Income'

#interest income line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestAndOtherIncome'),
'fsli'] = 'Interest Income'
IS_comm_credit.loc[(IS_comm_credit.fsli == 'Interest (Income)'),
'fsli'] = 'Interest Income'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestAndOtherIncome'),
'fsli'] = 'Interest Income'
IS_comm_credit.loc[(IS_comm_credit.fsli == 'Interest (Income)'),
'fsli'] = 'Interest Income'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestIncomeExpenseNet'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestIncomeExpenseNonoperatingNet'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InvestmentIncomeNet'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnInvestments'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestAndOtherIncome'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InterestIncomeOther'),
'fsli'] = 'Interest Income(Expense) Net'
IS_comm_credit.loc[(IS_comm_credit.tag == 'InvestmentIncomeNonoperating'),
'fsli'] = 'Interest Income(Expense) Net'

# other non operating, equity, continue or discontinue operations items
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherNonoperatingIncomeExpense'),
'fsli'] = 'Other Non-Operating Income(Expense)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherIncome'),
'fsli'] = 'Other Non-Operating Income(Expense)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherNonoperatingIncome'),
'fsli'] = 'Other Non-Operating Income(Expense)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherOperatingIncomeExpenseNet'),
'fsli'] = 'Other Operating Income(Expense)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherOperatingIncome'),
'fsli'] = 'Other Operating Income(Expense)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'NonoperatingIncomeExpense'),
'fsli'] = 'Non-Operating Income(Expense)'

IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromEquityMethodInvestments'),
'fsli'] = 'Equity Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromContinuingOperations'),
'fsli'] = 'Income(Loss) From Continued Operations'
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity'),
'fsli'] = 'Income(Loss) From Discontinued Operations'
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromDiscontinuedOperationsNetOfTax'),
'fsli'] = 'Income(Loss) From Discontinued Operations'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainsLossesOnExtinguishmentOfDebt'),
'fsli'] = IS_comm_credit['tlabel']
IS_comm_credit.loc[(IS_comm_credit.tag == 'IncomeLossFromDiscontinuedOperationsNetOfTax'),
'fsli'] = 'Income(Loss) From Discontinued Operations'
IS_comm_credit.loc[(IS_comm_credit.tag == 'ForeignCurrencyTransactionGainLossBeforeTax'),
'fsli'] = 'Foreign Exchange Gain(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'DerivativeGainLossOnDerivativeNet'),
'fsli'] = 'Foreign Exchange Gain(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnDerivativeInstrumentsNetPretax'),
'fsli'] = 'Gain(Loss) on Derivatives'

# OCI line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest'),
'fsli'] = 'Total Comprehensive Income(Loss) Including Non-Controlling Interest'
IS_comm_credit.loc[(IS_comm_credit.tag == 'ComprehensiveIncomeNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'ComprehensiveIncomeLossAttributableToNonControllingInterestsAndRedeemableNonControllingInterests'),
'fsli'] = 'Total Comprehensive Income(Loss) Atttributable to Non-Controlling Interest'

IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeLossForeignCurrencyTransactionAndTranslationAdjustmentNetOfTax'),
'fsli'] = 'Other Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeUnrealizedHoldingGainLossOnSecuritiesArisingDuringPeriodNetOfTax'),
'fsli'] = 'Other Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeLossNetOfTax'),
'fsli'] = 'Other Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'ComprehensiveIncomeNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeForeignCurrencyTransactionAndTranslationGainLossArisingDuringPeriodNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeLossAvailableForSaleSecuritiesAdjustmentNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeUnrealizedGainLossOnDerivativesArisingDuringPeriodNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'
IS_comm_credit.loc[(IS_comm_credit.tag == 'OtherComprehensiveIncomeLossPensionAndOtherPostretirementBenefitPlansAdjustmentNetOfTax'),
'fsli'] = 'Total Comprehensive Income(Loss)'


# Asset disposal line items
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnDispositionOfAssets1'),
'fsli'] = 'Gain(Loss) on Asset Sale'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnDispositionOfAssets'),
'fsli'] = 'Gain(Loss) on Asset Sale'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnSaleOfPropertyPlantEquipment'),
'fsli'] = 'Gain(Loss) on Asset Sale'
IS_comm_credit.loc[(IS_comm_credit.tag == 'GainLossOnSaleOfBusiness'),
'fsli'] = 'Gain(Loss) on Asset Sale'

# Commercial IS debit normalization

# COGS
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsAndServicesSold'),
'fsli'] = 'Cost of Goods Sold'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfRevenue'),
'fsli'] = 'Cost of Goods Sold'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsSold'),
'fsli'] = 'Cost of Goods Sold'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization'),
'fsli'] = 'Cost of Goods Sold'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization'),
'fsli'] = 'Cost of Goods Sold'

# Total COGS & operating Expenses
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostsAndExpenses'),
'fsli'] = 'Total COGS & Operating Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostsAndExpenses'),
'fsli'] = 'Total COGS & Operating Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OperatingCostsAndExpenses'),
'fsli'] = 'Total Operating Expenses'

#SG&A
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'SellingAndMarketingExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'SellingGeneralAndAdministrativeExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'GeneralAndAdministrativeExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'SellingExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'MarketingAndAdvertisingExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherGeneralAndAdministrativeExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherGeneralExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'AdvertisingExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'MarketingExpense'),
'fsli'] = 'SG&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherSellingGeneralAndAdministrativeExpense'),
'fsli'] = 'SG&A Expense'

# R&D
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ResearchAndDevelopmentExpense'),
'fsli'] = 'R&D Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost'),
'fsli'] = 'R&D Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ResearchAndDevelopmentInProcess'),
'fsli'] = 'R&D Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ResearchAndDevelopmentAssetAcquiredOtherThanThroughBusinessCombinationWrittenOff'),
'fsli'] = 'R&D Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost'),
'fsli'] = 'R&D Expense'

#D&A 
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DepreciationAndAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'AmortizationOfIntangibleAssets'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DepreciationDepletionAndAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'Depreciation'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsAndServicesSoldDepreciationAndAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherDepreciationAndAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsAndServicesSoldAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DepreciationAmortizationAndAccretionNet'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'UtilitiesOperatingExpenseDepreciationAndAmortization'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostOfGoodsAndServicesSoldDepreciation'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DepreciationNonproduction'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'AmortizationOfAcquiredIntangibleAssets'),
'fsli'] = 'D&A Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CostDepreciationAmortizationAndDepletion'),
'fsli'] = 'D&A Expense'

#Tax expense
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'IncomeTaxExpenseBenefit'),
'fsli'] = 'Tax Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'CurrentIncomeTaxExpenseBenefit'),
'fsli'] = 'Tax Expense'

# Other operating expenses
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'RestructuringCharges'),
'fsli'] = 'Restructuring Charges'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'RestructuringSettlementAndImpairmentProvisions'),
'fsli'] = 'Restructuring Charges'

IS_comm_Debit.loc[(IS_comm_Debit.tag == 'AssetImpairmentCharges'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfOilAndGasProperties'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherAssetImpairmentCharges'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfIntangibleAssetsIndefinitelivedExcludingGoodwill'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfLongLivedAssetsHeldForUse'),
'fsli'] = 'Asset Write-Down'

IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfIntangibleAssetsExcludingGoodwill'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfLongLivedAssetsToBeDisposedOf'),
'fsli'] = 'Asset Write-Down'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ImpairmentOfIntangibleAssetsFinitelived'),
'fsli'] = 'Asset Write-Down'

IS_comm_Debit.loc[(IS_comm_Debit.tag == 'GoodwillImpairmentLoss'),
'fsli'] = 'Goodwill Impairment'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'GoodwillAndIntangibleAssetImpairment'),
'fsli'] = 'Goodwill Impairment'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherCostAndExpenseOperating'),
'fsli'] = 'Other Operating Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherExpenses'),
'fsli'] = 'Other Operating Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'LaborAndRelatedExpense'),
'fsli'] = 'Other Operating Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'BusinessCombinationAcquisitionRelatedCosts'),
'fsli'] = 'Acquisition Related Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'BusinessCombinationContingentConsiderationArrangementsChangeInAmountOfContingentConsiderationLiability1'),
'fsli'] = 'Acquisition Related Expenses'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ShareBasedCompensation'),
'fsli'] = 'SBC Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'AllocatedShareBasedCompensationExpense'),
'fsli'] = 'SBC Expense'

#interest expense
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpense'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpenseDebt'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestAndDebtExpense'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpenseOther'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpenseRelatedParty'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpenseNet'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestIncomeExpenseNonoperatingNet'),
'fsli'] = 'Interest Expense'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'InterestExpenseDebtExcludingAmortization'),
'fsli'] = 'Interest Expense'


#other non operating interest expenses
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'NetIncomeLossAttributableToNoncontrollingInterest'),
'fsli'] = 'Net Income(Loss) Attributable to Non-Controlling Interest'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'IncomeLossAttributableToNoncontrollingInterest'),
'fsli'] = 'Net Income(Loss) Attributable to Non-Controlling Interest'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'NetIncomeLossAttributableToRedeemableNoncontrollingInterest'),
'fsli'] = 'Net Income(Loss) Attributable to Non-Controlling Interest'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'IncomeLossFromContinuingOperationsAttributableToNoncontrollingEntity'),
'fsli'] = 'Net Income(Loss) Attributable to Non-Controlling Interest'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'OtherNonoperatingExpense'),
'fsli'] = 'Other Non-Operating Expense'

# Preferred Stock Dividends
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'PreferredStockDividendsIncomeStatementImpact'),
'fsli'] = 'Preferred Stock Dividends'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'PreferredStockDividendsAndOtherAdjustments'),
'fsli'] = 'Preferred Stock Dividends'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DividendsPreferredStock'),
'fsli'] = 'Preferred Stock Dividends'
IS_comm_Debit.loc[(IS_comm_Debit.tag == 'DividendsPreferredStockStock'),
'fsli'] = 'Preferred Stock Dividends'

IS_comm_Debit.loc[(IS_comm_Debit.tag == 'ProvisionForDoubtfulAccounts'),
'fsli'] = 'Provision for Doubtful Accounts'

# commercial BS debit FSLIs

# Cash
BS_comm_debit.loc[(BS_comm_debit.tag == 'CashAndCashEquivalentsAtCarryingValue'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'ShortTermInvestments'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashAndCashEquivalentsAtCarryingValue'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'MarketableSecuritiesCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AvailableForSaleSecuritiesCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashAndCashEquivalents'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AvailableForSaleSecuritiesDebtSecuritiesCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'MarketableSecuritiesNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'Cash'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AvailableForSaleSecuritiesNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AvailableForSaleSecuritiesDebtSecuritiesNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AssetsHeldForSaleCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashAndInvestmentsNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'CashCashEquivalentsAndShortTermInvestments'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'HeldToMaturitySecuritiesCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'Investments'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'RestrictedCashAndInvestmentsCurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'MarketableSecurities'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'HeldToMaturitySecuritiesNoncurrent'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'OtherShortTermInvestments'),
'fsli'] = 'Cash & Equivalents'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AvailableForSaleSecuritiesDebtSecurities'),
'fsli'] = 'Cash & Equivalents'
# Available for sale securities

#AR
BS_comm_debit.loc[(BS_comm_debit.tag == 'AccountsReceivableNetCurrent'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'ReceivablesNetCurrent'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AccountsReceivableNet'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'NotesAndLoansReceivableNetNoncurrent'),
'fsli'] = 'Accounts Receivable'

BS_comm_debit.loc[(BS_comm_debit.tag == 'AccountsNotesAndLoansReceivableNetCurrent'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AccountsAndOtherReceivablesNetCurrent'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'NotesAndLoansReceivableNetCurrent'),
'fsli'] = 'Accounts Receivable'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AccountsReceivableRelatedPartiesCurrent'),
'fsli'] = 'Accounts Receivable'

# DTAs
BS_comm_debit.loc[(BS_comm_debit.plabel == 'DeferredTaxAssetsNetNoncurrent'),
'fsli'] = 'DTA NonCurrent'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'DeferredIncomeTaxAssetsNet'),
'fsli'] = 'DTA'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'DeferredTaxAssetsNetCurrent'),
'fsli'] = 'DTA Current'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'IncomeTaxesReceivable'),
'fsli'] = 'DTA'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'DeferredTaxAssetsLiabilitiesNetNoncurrent'),
'fsli'] = 'DTA NonCurrent'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'PrepaidTaxes'),
'fsli'] = 'DTA'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'DeferredTaxAssetsLiabilitiesNetCurrent'),
'fsli'] = 'DTA Current'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'IncomeTaxReceivable'),
'fsli'] = 'DTA'

# inventory
BS_comm_debit.loc[(BS_comm_debit.plabel == 'InventoryNet'),
'fsli'] = 'Inventory'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'AirlineRelatedInventoryNet'),
'fsli'] = 'Inventory'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'EnergyRelatedInventoryNaturalGasInStorage'),
'fsli'] = 'Inventory'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'InventoryRealEstate'),
'fsli'] = 'Inventory'

# accrued expenses
BS_comm_debit.loc[(BS_comm_debit.tag == 'AccruedExpensesAndOtherCurrentLiabilities'),
'fsli'] = 'Accrued Expenses'

# PPE
BS_comm_debit.loc[(BS_comm_debit.tag == 'PropertyPlantAndEquipmentNet'),
'fsli'] = 'PP&E'
BS_comm_debit.loc[(BS_comm_debit.plabel == 'Net property, plant and equipment'),
'fsli'] = 'PP&E'
BS_comm_debit.loc[(BS_comm_debit.tag == 'PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization'),
'fsli'] = 'PP&E'
BS_comm_debit.loc[(BS_comm_debit.tag == 'PropertyPlantAndEquipmentOtherNet'),
'fsli'] = 'PP&E'
BS_comm_debit.loc[(BS_comm_debit.tag == 'PublicUtilitiesPropertyPlantAndEquipmentPlantInService'),
'fsli'] = 'PP&E'


#Total CA & NCA
BS_comm_debit.loc[(BS_comm_debit.tag == 'AssetsCurrent'),
'fsli'] = 'Total Current Assets'
BS_comm_debit.loc[(BS_comm_debit.tag == 'AssetsNoncurrent'),
'fsli'] = 'Total NonCurrent Assets'

# long term investments
BS_comm_debit.loc[(BS_comm_debit.tag == 'LongTermInvestments'),
'fsli'] = 'Long Term Investments'
BS_comm_debit.loc[(BS_comm_debit.tag == 'OtherLongTermInvestments'),
'fsli'] = 'Long Term Investments'

# Goodwill
BS_comm_debit.loc[(BS_comm_debit.tag == 'Goodwill'),
'fsli'] = 'Goodwill'
BS_comm_debit.loc[(BS_comm_debit.tag == 'GoodwillAndIntangibleAssetsNet'),
'fsli'] = 'Goodwill'

#intangible Assets
BS_comm_debit.loc[(BS_comm_debit.tag == 'IntangibleAssetsNetExcludingGoodwill'),
'fsli'] = 'Intangible Assets'
BS_comm_debit.loc[(BS_comm_debit.tag == 'OtherIntangibleAssetsNet'),
'fsli'] = 'Intangible Assets'
BS_comm_debit.loc[(BS_comm_debit.tag == 'IntangibleAssetsAndOtherAssetsNoncurrent'),
'fsli'] = 'Intangible Assets'
BS_comm_debit.loc[(BS_comm_debit.tag == 'IntangibleAssetsNetAndOtherAssetsExcludingGoodwill'),
'fsli'] = 'Intangible Assets'
BS_comm_debit.loc[(BS_comm_debit.tag == 'FiniteLivedIntangibleAssetsNet'),
'fsli'] = 'Intangible Assets'

#total Assets
BS_comm_debit.loc[(BS_comm_debit.tag == 'Assets'),
'fsli'] = 'Assets'

#Trasury Stock
BS_comm_debit.loc[(BS_comm_debit.tag == 'TreasuryStockValue'),
'fsli'] = 'Treasury Stock'
BS_comm_debit.loc[(BS_comm_debit.tag == 'TreasuryStockCommonValue'),
'fsli'] = 'Treasury Stock'

# BS commercial credit fsli normalization

# AP
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableCurrent'),
'fsli'] = 'Accounts Payable'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableAndAccruedLiabilitiesCurrent'),
'fsli'] = 'Accounts Payable'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableTradeCurrent'),
'fsli'] = 'Accounts Payable'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableRelatedPartiesCurrent'),
'fsli'] = 'Accounts Payable'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableCurrentAndNoncurrent'),
'fsli'] = 'Accounts Payable'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccountsPayableOtherCurrent'),
'fsli'] = 'Accounts Payable'

#short term debt
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermDebtCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermDebtAndCapitalLeaseObligationsCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ShortTermBorrowings'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'DebtCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayableCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LinesOfCreditCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayableRelatedPartiesClassifiedCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ConvertibleNotesPayableCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LineOfCredit'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ShortTermBankLoansAndNotesPayable'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'SecuredDebtCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LoansPayableCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherLongTermDebtCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LoansPayableToBankCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'CommercialPaper'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherNotesPayableCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherShortTermBorrowings'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayableToBankCurrent'),
'fsli'] = 'Current Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesAndLoansPayableCurrent'),
'fsli'] = 'Current Debt'

# long term debt
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermDebtNoncurrent'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermDebtAndCapitalLeaseObligations'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermNotesPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermDebt'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermLineOfCredit'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ConvertibleLongTermNotesPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ConvertibleDebtNoncurrent'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherLongTermDebtNoncurrent'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayableRelatedPartiesNoncurrent'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'SeniorLongTermNotes'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'SecuredLongTermDebt'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermLoansPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'SeniorNotes'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LongTermLoansFromBank'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'UnsecuredLongTermDebt'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherLongTermNotesPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'SeniorUnsecuredNotes'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'NotesPayableToBankNoncurrent'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'ConvertibleNotesPayable'),
'fsli'] = 'NonCurrent Debt'
BS_comm_credit.loc[(BS_comm_credit.tag == 'OtherLoansPayableLongTerm'),
'fsli'] = 'NonCurrent Debt'

# total equity
BS_comm_credit.loc[(BS_comm_credit.tag == 'StockholdersEquity'),
'fsli'] = 'Shareholders Equity'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LiabilitiesAndStockholdersEquity'),
'fsli'] = 'Liabilities & Shareholders Equity'

# Accumulated OCI
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccumulatedOtherComprehensiveIncomeLossDefinedBenefitPensionAndOtherPostretirementPlansNetOfTax'),
'fsli'] = 'Accumulated OCI'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccumulatedOtherComprehensiveIncomeLossTax'),
'fsli'] = 'Accumulated OCI'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccumulatedOtherComprehensiveIncomeLossNetOfTax'),
'fsli'] = 'Accumulated OCI'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccumulatedOtherComprehensiveIncomeLossForeignCurrencyTranslationAdjustmentNetOfTax'),
'fsli'] = 'Accumulated OCI'

# total liabilities
BS_comm_credit.loc[(BS_comm_credit.tag == 'Liabilities'),
'fsli'] = 'Total Liabilities'
BS_comm_credit.loc[(BS_comm_credit.tag == 'LiabilitiesCurrent'),
'fsli'] = 'Current Liabilities'

# DTLs
BS_comm_credit.loc[(BS_comm_credit.tag == 'DeferredIncomeTaxLiabilitiesNet'),
'fsli'] = 'DTL'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccruedIncomeTaxesCurrent'),
'fsli'] = 'DTL Current'
BS_comm_credit.loc[(BS_comm_credit.tag == 'TaxesPayableCurrent'),
'fsli'] = 'DTL Current'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccruedIncomeTaxesNoncurrent'),
'fsli'] = 'DTL NonCurrent'
BS_comm_credit.loc[(BS_comm_credit.tag == 'DeferredTaxLiabilitiesCurrent'),
'fsli'] = 'DTL Current'
BS_comm_credit.loc[(BS_comm_credit.tag == 'DeferredIncomeTaxLiabilitiesNet'),
'fsli'] = 'DTL'
BS_comm_credit.loc[(BS_comm_credit.tag == 'DeferredTaxLiabilities'),
'fsli'] = 'DTL'
BS_comm_credit.loc[(BS_comm_credit.tag == 'TaxesPayableNoncurrent'),
'fsli'] = 'DTL NonCurrent'
BS_comm_credit.loc[(BS_comm_credit.tag == 'AccruedIncomeTaxes'),
'fsli'] = 'DTL'
BS_comm_credit.loc[(BS_comm_credit.tag == 'DeferredTaxAndOtherLiabilitiesNoncurrent'),
'fsli'] = 'DTL NonCurrent'
BS_comm_credit.loc[(BS_comm_credit.tag == 'TaxesPayableCurrentAndNoncurrent'),
'fsli'] = 'DTL'

# Preferred Stock
IS_comm_credit.loc[(IS_comm_credit.tag == 'PreferredStockValue'),
'fsli'] = 'Preferred Stock'


# retained earnings
BS_comm_credit.loc[(BS_comm_credit.tag == 'RetainedEarningsAccumulatedDeficit'),
'fsli'] = 'Retained Earnings'
BS_comm_credit.loc[(BS_comm_credit.tag == 'RetainedEarningsUnappropriated'),
'fsli'] = 'Retained Earnings'
BS_comm_credit.loc[(BS_comm_credit.tag == 'RetainedEarningsAppropriated'),
'fsli'] = 'Retained Earnings'


# commercial CF fsli normalization

# D&A
CF_comm.loc[(CF_comm.tag == 'DepreciationDepletionAndAmortization'),
'fsli'] = 'D&A Expense'
CF_comm.loc[(CF_comm.tag == 'DepreciationAndAmortization'),
'fsli'] = 'D&A Expense'
CF_comm.loc[(CF_comm.tag == 'Depreciation'),
'fsli'] = 'D&A Expense'
CF_comm.loc[(CF_comm.tag == 'AmortizationOfIntangibleAssets'),
'fsli'] = 'D&A Expense'

# dividends
CF_comm.loc[(CF_comm.tag == 'PaymentsOfDividends'),
'fsli'] = 'SBC'
CF_comm.loc[(CF_comm.tag == 'PaymentsOfDividendsCommonStock'),
'fsli'] = 'SBC'

# interest payments
CF_comm.loc[(CF_comm.tag == 'InterestPaid'),
'fsli'] = 'SBC'
CF_comm.loc[(CF_comm.tag == 'InterestPaidNet'),
'fsli'] = 'SBC'

#SBC
CF_comm.loc[(CF_comm.tag == 'ShareBasedCompensation'),
'fsli'] = 'SBC'


# capex
CF_comm.loc[(CF_comm.tag == 'PaymentsToAcquireProductiveAssets'),
'fsli'] = 'Capital Expenditures'
CF_comm.loc[(CF_comm.tag == 'PaymentsToAcquirePropertyPlantAndEquipment'),
'fsli'] = 'Capital Expenditures'

# sale of PP&E
CF_comm.loc[(CF_comm.tag == 'ProceedsFromSaleOfPropertyPlantAndEquipment'),
'fsli'] = 'Sale of PP&E'

#CFO
CF_comm.loc[(CF_comm.tag == 'NetCashProvidedByUsedInOperatingActivities'),
'fsli'] = 'CFO'

# financial institution specific fslis 

# interest income
df_financial.loc[(df_financial.tag == 'InterestIncomeExpenseNet'),
'fsli'] = 'Interest Income'
df_financial.loc[(df_financial.tag == 'InterestAndDividendIncomeOperating'),
'fsli'] = 'Interest Income'
df_financial.loc[(df_financial.tag == 'InterestIncomeExpenseAfterProvisionForLoanLoss'),
'fsli'] = 'Interest Income'
df_financial.loc[(df_financial.tag == 'InterestAndFeeIncomeLoansAndLeases'),
'fsli'] = 'Interest Income'

# non-interest expense
df_financial.loc[(df_financial.tag == 'InterestAndFeeIncomeLoansAndLeases'),
'fsli'] = 'Non-Interest Expense'

# provision for credit losses
df_financial.loc[(df_financial.tag == 'ProvisionForLoanLeaseAndOtherLosses'),
'fsli'] = 'Provision for Credit Losses'

# net loans & acceptances
df_financial.loc[(df_financial.tag == 'LoansAndLeasesReceivableNetReportedAmount'),
'fsli'] = 'Net Loans and Acceptances'

NoActg.loc[(NoActg.tag == 'EarningsPerShareDiluted'), 'fsli'] = 'EPS Diluted'

NoActg.loc[NoActg.tag.isin(['EarningsPerShareBasic', 'EarningsPerShareBasicAndDiluted']), 'fsli'] = 'EPS Basic'

NoActg.loc[NoActg.tag.isin(['WeightedAverageNumberOfSharesOutstandingBasic', 
'WeightedAverageNumberOfShareOutstandingBasicAndDiluted', 
'WeightedAverageNumberOfLimitedPartnershipAndGeneralPartnershipUnitOutstandingBasicAndDiluted', 
'CommonStockSharesOutstanding', 'WeightedAverageNumberOfSharesOutstandingBasicAndDiluted1']), 'fsli'] = 'Basic Shares Outstanding'

NoActg.loc[NoActg.tag.isin(['WeightedAverageNumberOfDilutedSharesOutstanding', 
'WeightedAverageLimitedPartnershipUnitsOutstandingDiluted']), 'fsli'] = 'Diluted Shares Outstanding'

NoActg.loc[(NoActg.tag == 'CommonStockDividendsPerShareCashPaid'), 'fsli'] = 'Dividends Per Share'

# Comnbine Arrays, drop duplicates, drop commitments&Contingencies since its always empty
combinedArray = [NoActg, df_financial, CF_comm, IS_comm_Debit, IS_comm_credit, IS_comm_Debit, BS_comm_debit, BS_comm_credit]
df10 = pd.concat(combinedArray)
df10 = df10.drop_duplicates()
df10 = df10[df10.tag != 'CommitmentsAndContingencies']
df10 = df10[df10.year != 2007]
df10 = df10[df10.year != 2008]
df10 = df10[df10.year != 2009]
# Calc Q4 values

df10['year_period'] = df10['year'].astype(str) + '_' + df10['fiscal_period']

# for index, row in df10.iterrows():
#     for ticker in row:

df12 = df10[['ticker', 'stmt', 'year', 'plabel', 'fsli', 'qtrs', 'crdr', 'fiscal_period', 'year_period', 'num_value']]
df12 = df12.drop_duplicates()


df16 = df12[df12.stmt == 'BS']
df16 = df16.dropna(subset=['num_value'])
df17 = df16[['ticker', 'stmt', 'year', 'plabel', 'fsli', 'qtrs', 'crdr', 'fiscal_period', 'num_value']]

df15 = df12[df12.stmt != 'BS']
df13 = df15.pivot_table(index=['ticker', 'stmt', 'plabel', 'fsli', 'qtrs', 'crdr', 'year', 'fiscal_period'], columns='year_period', values='num_value')
df13['2020.0_Q4'] = df13['2020.0_FY'] - df13['2020.0_Q3'] - df13['2020.0_Q2'] - df13['2020.0_Q1']
df13['2019.0_Q4'] = df13['2019.0_FY'] - df13['2019.0_Q3'] - df13['2019.0_Q2'] - df13['2019.0_Q1']
df13['2018.0_Q4'] = df13['2018.0_FY'] - df13['2018.0_Q3'] - df13['2018.0_Q2'] - df13['2018.0_Q1']
df13['2017.0_Q4'] = df13['2017.0_FY'] - df13['2017.0_Q3'] - df13['2017.0_Q2'] - df13['2017.0_Q1']
df13['2016.0_Q4'] = df13['2016.0_FY'] - df13['2016.0_Q3'] - df13['2016.0_Q2'] - df13['2016.0_Q1']
df13['2015.0_Q4'] = df13['2015.0_FY'] - df13['2015.0_Q3'] - df13['2015.0_Q2'] - df13['2015.0_Q1']
df13['2014.0_Q4'] = df13['2014.0_FY'] - df13['2014.0_Q3'] - df13['2014.0_Q2'] - df13['2014.0_Q1']
df13['2013.0_Q4'] = df13['2013.0_FY'] - df13['2013.0_Q3'] - df13['2013.0_Q2'] - df13['2013.0_Q1']
df13['2012.0_Q4'] = df13['2012.0_FY'] - df13['2012.0_Q3'] - df13['2012.0_Q2'] - df13['2012.0_Q1']
df13['2011.0_Q4'] = df13['2011.0_FY'] - df13['2011.0_Q3'] - df13['2011.0_Q2'] - df13['2011.0_Q1']
df13['2010.0_Q4'] = df13['2010.0_FY'] - df13['2010.0_Q3'] - df13['2010.0_Q2'] - df13['2010.0_Q1']

df13 = df13.reset_index()

dfnotsure = pd.melt(df13, id_vars=['ticker', 'stmt', 'plabel', 'fsli', 'year', 'fiscal_period', 'qtrs', 'crdr'], var_name='year_period', value_name='num_value')
dfnotsure = dfnotsure.drop(columns=['year_period'], axis=1)
dfnotsure = dfnotsure.dropna(subset=['num_value'])

finalConcat = [dfnotsure, df17]

finalOutput = pd.concat(finalConcat)
finalOutput.to_sql('outPutSample', engine, if_exists='append', index=False)
print(finalOutput)
print('All done! :)')
