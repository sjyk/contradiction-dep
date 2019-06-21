
def match(df1, df2, columns):
    mdf = df1.merge(df2, on=columns, suffixes=('', '_right'))
    for c in columns:
        mdf[c+'_right'] = mdf[c]
    return mdf

