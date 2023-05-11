import pandas as pd


def merge_dataframes(list_of_df):
    list_of_df = ensure_list(list_of_df)
    # concatenate multiple dataframes, group by airframe hex pick last NaN value for each
    df = pd.concat(list_of_df)
    df = df.groupby(['hex']).last().reset_index()
    # todo add checks
    #fields https://github.com/wiedehopf/readsb/blob/dev/README-json.md#aircraftjson
    #todo handle the fact that 'calc_track' is not always present in the response, perhaps on flux query level
    df = df.drop(columns=['result', 'table'])
    return df

# ensures object is of type list, depending on traffic, a dataset or a list of datasets can be returned, breaking the concat
def ensure_list(item):
    if isinstance(item, list):
        return item
    else:
        return [item]
