import pandas as pd


def merge_dataframes(list_of_df):
    # concatenate multiple dataframes, group by airframe hex pick last NaN value for each
    df = pd.concat(list_of_df)
    df = df.groupby(['hex']).last().reset_index()
    # todo add checks
    #fields https://github.com/wiedehopf/readsb/blob/dev/README-json.md#aircraftjson
    #todo had to drop calc_track, where did this field come from? was not in initial queries
    df = df.drop(columns=['result', 'table', 'calc_track'])
    return df
