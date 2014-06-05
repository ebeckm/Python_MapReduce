import pandas, numpy, scipy

def build_id_column(df,keys,col_name):
    res = df[keys].astype(int).astype(str)
    res = res.sum(axis=1).astype(int)
    res = pandas.DataFrame(res)
    res.columns = [col_name]
    res = pandas.concat([df,res],axis=1)
    return res

def map_tf_to_10(x):
    return 1 if x=='t' else 0

def pull_normalization_constants(df):
    results = {}
    for col in df:
        results[col]={
            'mean' : df[col].mean(),
            'var'  : df[col].std() }
    return results

def apply_normalization_constants(df,norm):
    for col,cons in norm.items():
        df[col] = (df[col]-cons['mean'])/cons['var']
