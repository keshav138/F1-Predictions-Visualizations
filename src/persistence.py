import os,json
import pandas as pd


def save_raw_json(rows, season, gp, root='data'):
    root = os.path.join(root,'raw')
    os.makedirs(root, exist_ok=True)

    path = os.path.join(root,f'season_{season}_round_{gp}_raw.json')
    with open(path, 'w', encoding='utf8') as f:
        json.dump(rows,f,default=str, indent=2)
    
    return path



def save_raw_csv(df_raw, season, gp, root='data'):
    
    root = os.path.join(root,'raw')
    os.makedirs(root, exist_ok=True)
    
    path = os.path.join(root, f'season_{season}_round_{gp}_raw.csv')
    df_raw.to_csv(path, index=False)
    return path


def save_clean(df_work, season , gp, root='data'):
    
    root = os.path.join(root,'clean')
    os.makedirs(root, exist_ok=True)
    
    path_json = os.path.join(root,f'season_{season}_round_{gp}_clean.json')
    path_csv = os.path.join(root,f'season_{season}_round_{gp}_clean.csv')
    
    df_work.to_csv(path_csv, index=False)
    
    records = df_work.to_dict(orient='records')
    
    with open(path_json, 'w', encoding='utf8') as f:
        json.dump(records,f,indent=2, default=str)

    return path_csv, path_json
