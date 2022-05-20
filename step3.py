import pandas as pd
from pandas.core.groupby import groupby
import sqlalchemy
import numpy as np
import hidden

engine = sqlalchemy.create_engine(hidden.db_route)
df = pd.read_sql_table('outPutSample', engine)
df1 = df[['ticker', 'fsli', 'year', 'fiscal_period', 'num_value']]
# uniquefslis = df1['fsli'].unique()
# unqf = pd.DataFrame(uniquefslis)
# unqf.to_csv(r'C:\Users\chris\Desktop\uniqueFSLISmetrics.csv')

# Revenue Growth
# revenue = df1[df1['fsli'] == 'Net Revenue']
# revenue = revenue.drop_duplicates()
# AnnRev = revenue[revenue['fiscal_period'] == 'FY']
# AnnRev['revGrowth'] = AnnRev.sort_values(['year']).groupby('ticker')['num_value'].pct_change()
# revGrowth_df = AnnRev[['ticker', 'year', 'revGrowth']].dropna()
# revGrowth_df = revGrowth_df.rename(columns={'revGrowth': 'num_value'})
# revGrowth_df['type'] = 'growth rate'
# revGrowth_df['fsli'] = 'Annual Revenue Growth'
# revGrowth_df['fiscal_period] = 'FY'
# revGrowth_df = revGrowth_df[['ticker', 'type', 'fsli', 'year', 'period', 'num_value']]

# Current Ratio
currAss = df1[df1['fsli'] == 'Total Current Assets']
currLi = df1[df1['fsli'] == 'Current Liabilities']
CaR = pd.merge(currAss, currLi, how='left', on=['ticker', 'year', 'fiscal_period'])
CaR = CaR.drop_duplicates()
CaR['num_value'] = CaR['num_value_x'] / CaR['num_value_y']
CaR['type'] = 'liquidity ratio'
CaR['fsli'] = 'Current Ratio'
CaR = CaR[['ticker', 'type', 'fsli', 'year', 'fiscal_period', 'num_value']]

