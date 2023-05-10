import pandas as pd


def merge_dataframes(list):
    # concatenate multiple dataframes, group by airframe hex pick last NaN value for each
    df = pd.concat(list)
    df = df.groupby(['hex']).last().reset_index()
    # todo add checks
    print(df.to_string())
    return df
