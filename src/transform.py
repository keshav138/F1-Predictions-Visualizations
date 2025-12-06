import numpy as np
import pandas as pd


def compute_derived(df_raw):
    df_work = df_raw.copy()
    
    '''
        Calculates lap time delta
    '''
    ##--------------------------------------------------------------------------------------------------------------
    df_work = df_work.sort_values(by=['driver_name', 'lap_number']).reset_index(drop=True)
    df_work['lap_time_delta'] = np.nan

    for driver, g in df_work.groupby('driver_name', sort=False):
        valid_idx = g[g['lap_time'].notna()].index
        
        if(len(valid_idx) <= 1) : continue
        
        delta = df_work.loc[valid_idx, 'lap_time'].diff().values
        df_work.loc[valid_idx, 'lap_time_delta'] = delta
        
    df_work.loc[df_work['is_inlap'] | df_work['is_outlap'], 'lap_time_delta'] = np.nan
    ##--------------------------------------------------------------------------------------------------------------
    
    '''
        Calculate the best lap time on current tyre
    '''
    ##--------------------------------------------------------------------------------------------------------------
    
    df_work['lap_time_best_on_tyre'] = False

    df_work['stint_min_lap'] = df_work.groupby(['driver_name','stint_number'])['lap_time'].transform('min')
    
    df_work.loc[
        (df_work['lap_time']==df_work['stint_min_lap']) &
        (df_work['lap_time'].notna()),
        'lap_time_best_on_tyre'
    ] = True
    
    df_work.drop(columns=['stint_min_lap'], inplace=True)
    
    return df_work
    ##--------------------------------------------------------------------------------------------------------------

    
def normalise(df, mapping=None):
    if mapping is None:
        mapping = {
            'SOFT':'Soft',
            'MEDIUM':'Medium',
            'INTERMEDIATE':'Intermediate',
            'WET':'Wet',
            'HARD':'Hard'
        }
        
    df['compound'] = df['compound'].astype(str).str.upper().map(mapping).fillna(df['compound'])
    '''
        We do fillna in the end since if a value appears that's not in the mapping , its output is NaN, which we dont want, so
        to retain original values, we do fillna(df['compound'])
    '''
    return df