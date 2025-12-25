import pandas as pd
import numpy as np

def add_lap_features(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Adds basic lap features
    '''
    
    df_add = df.copy()
    
    ## 1. checking if lap number exists
    if 'lap_number' in df.columns:
        df_add['lap_number'] = df_add['lap_number'].astype('Int64') ## convert floats to int for round up calculations
        
    # 2. calculate laptimedelta if missing or incomplete
    if 'lap_time_delta' not in df_add.columns or df_add['lap_time_delta'].isna().any():
        df_add['lap_time_delta'] = df_add.groupby('driver_name')['lap_time'].diff()
    else:
        df_add['lap_time_delta'] = df_add['lap_time_delta']
    
    if 'lap_number' in df_add.columns and df_add['lap_number'].notna().any():
        max_lap = int(df_add['lap_number'].max())
        df_add['lap_frac'] = df_add['lap_number'] / max_lap
        df_add['total_laps'] = max_lap    
    else:
        df_add['lap_frac'] = np.nan
        df_add['total_laps'] = np.nan
    
    return df_add


def add_telemetry_features(df : pd.DataFrame, rolling : int = 3) -> pd.DataFrame:
    '''
    Telemetry aggregates and rolling windows for mean
    '''
    
    df_add = df.copy()
    
    telemetry_cols = ['avg_speed', 'avg_throttle', 'avg_brake','avg_gear', 'max_rpm', 'max_speed', 'std_throttle','std_brake']
    
    for c in telemetry_cols:
        if c not in df_add.columns:
            df_add[c] = np.nan
            
            
    df_add = df_add.sort_values(['driver_name','lap_number'])
    
    roll = lambda s: s.rolling(rolling, min_periods=1).mean()
    
    df_add[f'rolling_avg_speed_{rolling}'] = df_add.groupby('driver_name')['avg_speed'].transform(roll)
    df_add[f'rolling_avg_throttle_{rolling}'] = df_add.groupby('driver_name')['avg_throttle'].transform(roll)
    df_add[f'rolling_avg_brake_{rolling}'] = df_add.groupby('driver_name')['avg_brake'].transform(roll)
    
    df_add[f'throttle_brake_ratio'] = df_add['avg_throttle'] / (df_add['avg_brake']+ 1e-6)
    
    return df_add



def add_tyre_features(df : pd.DataFrame) -> pd.DataFrame:
    '''
    Docstring for add_tyre_features
    
    :param df: recieves a dataFrame, returns the required tyre features, like tyre age , stint, best lap times
    :type df: pd.DataFrame
    :return: Description
    :rtype: DataFrame
    '''
    
    df_add = df.copy()    
    
    if 'tyre_age' not in df_add.columns:
        df_add['tyre_age'] = np.nan
        
    def stint_phase(a):
        if pd.isna(a):
            return 'unknown'
        if a <= 2:
            return 'warmup'
        if 3<=a<=12:
            return 'stable'
        return 'degradation'
    

    df_add.loc[:,'stint_phase'] = df_add['tyre_age'].apply(stint_phase)
    
    if 'lap_time_best_on_tyre' in df.columns:
        df_add['lap_time_best_on_tyre'] = df_add['lap_time_best_on_tyre'].astype('bool')
    else:
        df_add['lap_time_best_on_tyre'] = False
        
    return df_add


def add_driver_features(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    '''
        driver level rolling features
        - driver_avg_pace
        - driver_consistency : rolling std of lap times
        - pace_deviation : lap_time - driver_avg_pace
    '''
    
    df_add = df.copy()
    
    group_cols = []
    
    if 'season' in df.columns:
        group_cols.append('season')
    if 'round' in df.columns:
        group_cols.append('round')
    
    ## driver level mean per race    
    key = group_cols + ['driver_name'] if group_cols else ['driver_name'] ## identifies one driver for 52ish laps in one race
    
    if 'lap_time' in df_add.columns:
        df_add.loc[:,'driver_avg_pace'] = df_add.groupby(key)['lap_time'].transform('mean')
    else:
        df_add.loc[:,'driver_avg_pace'] = np.nan
        
    
    ## rolling std of lap_time per racer
    
    df_add = df_add.sort_values(['driver_name','lap_number'])
    ## instantly calculate the rolling standard deviation of lap times
    
    if 'lap_time' in df_add.columns:
        df_add.loc[:, f'driver_consistency_{window}'] = df_add.groupby('driver_name')['lap_time'].transform(lambda x: x.rolling(window, min_periods=1).std().fillna(0))
    else:
        df_add.loc[:,f'driver_consistency_{window}'] = np.nan
    
    
    ## pace deviation
    df_add.loc[:, 'pace_deviation'] = df_add['lap_time'] - df_add['driver_avg_pace']
    
    return df_add


def add_compound_features( df: pd.DataFrame) -> pd.DataFrame:
    '''
        Generate one hot encoding for compounds
        compound average pace and deviation
    '''
    
    df_add = df.copy()
    
    if 'compound' not in df_add.columns:
        df_add['compound'] = ''
    
    df_add['compound'] = df_add['compound'].astype(str).str.strip().str.lower()
    
    def normalize_comp(c):
        if 'soft' in c:
            return 'soft'
        if 'medium' in c:
            return 'medium'
        if 'hard' in c:
            return 'hard'
        return 'other'
    
    df_add['compound_cat'] =  df_add['compound'].apply(normalize_comp)
    
    ## one hot
    
    one_hot = pd.get_dummies(df_add['compound_cat'], prefix='compound')
    
    ##----------------------------------------------------------------------
    df_add = pd.concat([df_add,one_hot], axis = 1)
    ### ---------------------------------------------------------------------
    
    group_cols = []
    if 'season' in df_add.columns:
        group_cols.append('season')
    if 'round' in df_add.columns:
        group_cols.append('round')
        
    group_cols = group_cols + ['compound_cat']
    if 'lap_time' in df_add.columns:
        df_add.loc[:,'compound_average_pace'] = df_add.groupby(group_cols)['lap_time'].transform('mean') ## average pace by 
        df_add.loc[:,'compound_pace_deviation'] = df_add['lap_time'] - df_add['compound_average_pace']
    else:
        df_add.loc[:,'compound_average_pace'] = np.nan
        df_add.loc[:,'compound_pace_deviation'] = np.nan
    
    return df_add


def finalize_feature_matrix(df: pd.DataFrame, drop_cols = None) -> pd.DataFrame:
    '''
        Final cleaning , removing unused columns, fill na, encode stint_phase
    '''
    
    final_df = df.copy()
    
    # drop cols
    if drop_cols is None:
         drop_cols = ['gap_to_leader', 'circuit_name']
         
    for col in drop_cols:
        if col in df.columns:
            final_df.drop(columns=[col], inplace = True)
            
            
    ## filling numerical columns with median
    nums_cols = final_df.select_dtypes(include='number').columns
    if len(nums_cols) > 0:
        final_df[nums_cols] = final_df[nums_cols].fillna(final_df[nums_cols].median())
        
            
    ## encode stint 
    if 'stint_phase' in final_df.columns:
        phase_map = {
            'unknown' : 0,
            'warmup' : 1,
            'stable' : 2,
            'degradation' : 3,
        }
        final_df['stint_phase_code'] = final_df['stint_phase'].map(phase_map).fillna(0).astype(int)
    
    ## reordering the dataframe to make sure target lap time is in the endd
    
    if 'lap_time' in final_df.columns:
        cols = [c for c in final_df.columns if c!='lap_time'] + ['lap_time']
        final_df = final_df[cols]
        
    return final_df