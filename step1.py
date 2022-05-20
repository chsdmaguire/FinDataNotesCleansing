import pandas as pd
import sqlalchemy
from functools import reduce
import os
import hidden

engine = sqlalchemy.create_engine(hidden.db_route)
df10 = pd.read_sql_table('basic_info', engine)
df1 = df10.iloc[:, 0].str.upper()

def add_fp(df):
    if df['fp'] == 'Q1' or df['fp'] == 'Q2' or df['fp'] == 'Q3' or df['fp'] == 'Q4' or df['fp'] == 'H1' or df['fp'] == 'M9':

        if df['fye'] <= 931 and df['fye'] >= 915:
                if df['month_day'] >= 1215 and df['month_day'] <= 1231:
                        return'Q1'
                elif df['month_day'] >= 315 or df['month_day'] <= 410:
                        return 'Q2'
                elif df['month_day'] >= 615 or df['month_day'] <= 731:
                        return 'Q3'
                elif df['month_day'] >= 915 or df['month_day'] <= 931:
                    return 'Q4'

        elif df['fye'] <= 831 and df['fye'] >= 815:
                if df['month_day'] >= 1115 and df['month_day'] <= 1210:
                        return'Q1'
                elif df['month_day'] >= 215 or df['month_day'] <= 310:
                        return 'Q2'
                elif df['month_day'] >= 515 or df['month_day'] <= 631:
                        return 'Q3'
                elif df['month_day'] >= 815 or df['month_day'] <= 831:
                    return 'Q4'

        elif df['fye'] <= 331 and df['fye'] >= 315:
                if df['month_day'] >= 615 and df['month_day'] <= 710:
                        return'Q1'
                elif df['month_day'] >= 915 or df['month_day'] <= 1010:
                        return 'Q2'
                elif df['month_day'] >= 1215 or df['month_day'] <= 1231:
                        return 'Q3'
                elif df['month_day'] >= 315 or df['month_day'] <= 331:
                    return 'Q4'

        elif df['fye'] <= 531 and df['fye'] >= 515:
                if df['month_day'] >= 815 and df['month_day'] <= 910:
                        return'Q1'
                elif df['month_day'] >= 1115 or df['month_day'] <= 1210:
                        return 'Q2'
                elif df['month_day'] >= 215 or df['month_day'] <= 310:
                        return 'Q3'
                elif df['month_day'] >= 515 or df['month_day'] <= 531:
                    return 'Q4'

        elif df['fye'] <= 631 and df['fye'] >= 615:
                if df['month_day'] >= 915 and df['month_day'] <= 1010:
                        return'Q1'
                elif df['month_day'] >= 1215 or df['month_day'] <= 1231:
                        return 'Q2'
                elif df['month_day'] >= 315 or df['month_day'] <= 410:
                        return 'Q3'
                elif df['month_day'] >= 615 or df['month_day'] <= 631:
                    return 'Q4'

        elif df['fye'] <= 131 and df['fye'] >= 115:
                if df['month_day'] >= 415 and df['month_day'] <= 510:
                        return'Q1'
                elif df['month_day'] >= 715 or df['month_day'] <= 810:
                        return 'Q2'
                elif df['month_day'] >= 1015 or df['month_day'] <= 1110:
                        return 'Q3'
                elif df['month_day'] >= 115 or df['month_day'] <= 131:
                    return 'Q4'

        elif df['fye'] <= 231 and df['fye'] >= 215:
                if df['month_day'] >= 515 and df['month_day'] <= 510:
                        return'Q1'
                elif df['month_day'] >= 815 or df['month_day'] <= 810:
                        return 'Q2'
                elif df['month_day'] >= 1115 or df['month_day'] <= 1210:
                        return 'Q3'
                elif df['month_day'] >= 915 or df['month_day'] <= 931:
                    return 'Q4'

        elif df['fye'] <= 431 and df['fye'] >= 415:
                if df['month_day'] >= 715 and df['month_day'] <= 110:
                        return'Q1'
                elif df['month_day'] >= 1015 or df['month_day'] <= 1110:
                        return 'Q2'
                elif df['month_day'] >= 115 or df['month_day'] <= 210:
                        return 'Q3'
                elif df['month_day'] >= 415 or df['month_day'] <= 431:
                        return 'Q4'

        elif df['fye'] <= 731 and df['fye'] >= 715:
            if df['month_day'] >= 1015 and df['month_day'] <= 1010:
                    return'Q1'
            elif df['month_day'] >= 115 or df['month_day'] <= 210:
                    return 'Q2'
            elif df['month_day'] >= 415 or df['month_day'] <= 510:
                    return 'Q3'
            elif df['month_day'] >= 715 or df['month_day'] <= 731:
                    return 'Q4'

        elif df['fye'] <= 1031 and df['fye'] >= 1015:
            if df['month_day'] >= 115 and df['month_day'] <= 210:
                    return'Q1'
            elif df['month_day'] >= 415 or df['month_day'] <= 510:
                    return 'Q2'
            elif df['month_day'] >= 715 or df['month_day'] <= 810:
                    return 'Q3'
            elif df['month_day'] >= 1015 or df['month_day'] <= 1031:
                    return 'Q4'

        elif df['fye'] <= 1131 and df['fye'] >= 1115:
            if df['month_day'] >= 215 and df['month_day'] <= 310:
                    return'Q1'
            elif df['month_day'] >= 515 or df['month_day'] <= 610:
                    return 'Q2'
            elif df['month_day'] >= 815 or df['month_day'] <= 910:
                    return 'Q3'
            elif df['month_day'] >= 1115 or df['month_day'] <= 1131:
                    return 'Q4'

        elif df['fye'] <= 1231 and df['fye'] >= 1215:
            if df['month_day'] >= 315 and df['month_day'] <= 405:
                    return'Q1'
            elif df['month_day'] >= 615 and df['month_day'] <= 710:
                    return 'Q2'
            elif df['month_day'] >= 915 and df['month_day'] <= 1010:
                    return 'Q3'
            elif df['month_day'] >= 1215 or df['month_day'] <= 1231:
                    return 'Q4'

    elif df['fp'] == 'FY' or df['fp'] == 'CY':
        return 'FY'

def comm_financial(df):
    if df['sic'] >= 6021 and df['sic'] <= 6411:
        return 'financial'
    elif df['sic'] >= 6500 and df['sic'] <= 7011:
        return 'real estate'
    else:
        return 'commercial'

rootdir = r'C:\Users\chris\Desktop\FinData_withNotes'
for filename in os.listdir(rootdir):
    # get ticker values & merge with ticker list from psql table
    textDoc = pd.read_csv(r'{}\txt.tsv'.format(os.path.join(rootdir, filename)), sep="\t", index_col=False, error_bad_lines=False)
    textDoc = textDoc[['adsh', 'tag', 'version', 'ddate', 'dimh', 'qtrs', 'lang', 'coreg', 'value']]
    tickers = textDoc.loc[textDoc['tag'] == 'TradingSymbol', ['value', 'adsh']]
    tickers.rename(columns={'value': 'ticker'}, inplace=True)
    ticker_merge = pd.merge(df1, tickers, how='inner', on='ticker')

    # get just important columns from sub & pre & 3 way merge
    subDoc = pd.read_csv(r'{}\sub.tsv'.format(os.path.join(rootdir, filename)), sep="\t", index_col=False, error_bad_lines=False)
    preDoc = pd.read_csv(r'{}\pre.tsv'.format(os.path.join(rootdir, filename)), sep="\t", index_col=False, error_bad_lines=False)
    subNew = subDoc[['adsh', 'name', 'cik', 'sic', 'fye', 'form', 'fp', 'pubfloatusd', 'prevrpt']]
    preDoc = preDoc.drop(['prole', 'negating'], axis=1)
    dfs = [ticker_merge, subNew, preDoc]
    tickerSubPre = reduce(lambda left, right: pd.merge(left, right, on='adsh'), dfs)
    tickerSubPre = tickerSubPre.drop_duplicates()

    # get important columns from tag doc and merge
    tagDoc = pd.read_csv(r'{}\tag.tsv'.format(os.path.join(rootdir, filename)), sep="\t", index_col=False, error_bad_lines=False)
    tagDoc = tagDoc[['tag', 'datatype', 'crdr', 'tlabel']]
    tagMerge = pd.merge(tickerSubPre, tagDoc, how='left', on='tag')

    # get important columns from txt doc, get only rows where dimh = 0x00000000 
    # rename lang column to match with num column, & merge
    newText = textDoc.loc[textDoc.dimh == '0x00000000']
    newText = newText.drop(columns=['dimh'])
    newText = newText[newText['qtrs'].isin([0, 1, 4])]
    textDoc = textDoc.rename(columns={'lang': 'uom_lang', 'value': 'text_value'})
    textMerge = pd.merge(tagMerge, textDoc, how='inner', on=['adsh', 'tag', 'version'])
    texttMerge = textMerge.drop_duplicates()

    # get important columns from num doc, get only rows where dimh = 0x00000000 
    # rename up, get only qrts with 0, 1, 4 & column to match with num column, & merge
    numDoc = pd.read_csv(r'{}\num.tsv'.format(os.path.join(rootdir, filename)), sep="\t", index_col=False, error_bad_lines=False)
    numDoc = numDoc[['adsh', 'tag', 'version', 'ddate', 'qtrs', 'dimh', 'uom', 'coreg', 'value']]
    numDoc = numDoc.loc[numDoc.dimh == '0x00000000']
    numDoc = numDoc[numDoc['qtrs'].isin([0, 1, 4])]
    numDoc = numDoc.drop(columns=['dimh'])
    numDoc = numDoc.rename(columns={'uom': 'uom_lang', 'value': 'num_value'})
    numMerge = pd.merge(tagMerge, numDoc, how='inner', on=['adsh', 'tag'])
    numMerge = numMerge.drop_duplicates()

    # concat num and text merge dfs together, & run functions through them
    dataConcat = pd.concat([numMerge, textMerge], ignore_index=True)
    dataConcat['ddate'] = dataConcat['ddate'].astype(str)
    dataConcat['name'] = dataConcat['name'].str.lower()
    dataConcat['ticker'] = dataConcat['ticker'].str.upper()
    dataConcat['name'] = dataConcat['name'].str.capitalize()
    dataConcat['year'] = dataConcat['ddate'].str[0:4]
    dataConcat['month_day'] = dataConcat['ddate'].str[4:8]
    dataConcat['month_day'] = dataConcat['month_day'].astype(int)
    dataConcat['fiscal_period'] = dataConcat.apply(add_fp, axis=1)
    dataConcat['classification'] = dataConcat.apply(comm_financial, axis=1)
    dataConcat = dataConcat.drop(columns=['ddate', 'fp', 'dimh', 'version_x', 'version_y', 'version'], axis=1)
    dataConcat = dataConcat.drop_duplicates()

    dataConcat.to_sql('findatawithnotes', engine, if_exists='append', index=False)
    print(filename + '--great success!')
