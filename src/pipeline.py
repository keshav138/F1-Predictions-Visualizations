import fastf1
import time
import glob
import os
import pandas as pd
import json
import config

from ingestion import load_session, extract_rows_from_session
from transform import compute_derived, normalise
from persistence import save_clean, save_raw_csv, save_raw_json

def process_round(season, gp):
    
    start_time = time.time()
    print(f'   -> Starting round {gp} at time {time.strftime('%H:%M:%S')}')
    
    project_root = r'C:\Users\ASUS\Desktop\F1 Predictions & Visualizations\F1-ML-Project'
    root = os.path.join(project_root, 'data')
    
    session  = load_session(season, gp)
    
    rows, df_raw = extract_rows_from_session(session, season, gp)
    
    raw_path_json = save_raw_json(rows,season, gp, root)
    raw_path_csv = save_raw_csv(df_raw, season, gp, root)
     
    
    df_work = compute_derived(df_raw)
    df_work = normalise(df_work)
    
    ##------------------------------------------------------------------
    assert df_work['driver_name'].notna().all() , "Missing Driver Name"
    assert 'lap_time' in df_work.columns, 'lap time missing'
    assert df_work['lap_time'].isna().mean() < 0.5, 'Too many NaN lap_times'
    ##------------------------------------------------------------------
    
    clean_csv,clean_json = save_clean(df_work, season, gp, root)
    
    ## report
    report = {
        'season' : season,
        'round': gp,
        'rows_raw': len(rows),
        'rows_clean' :len(df_work)
    }
    
    logs_dir = os.path.join(root,'pipeline_logs')
    os.makedirs(logs_dir, exist_ok=True)
    pipeline_logs_path = os.path.join(logs_dir,f'round_{gp}_report.json')
    
    with open(pipeline_logs_path, 'w', encoding='utf8') as f:
        json.dump(report, f, indent = 2)
    
    elapsed_time = time.time() - start_time
    print(f'  Finished round {gp} in {elapsed_time:.2f} seconds\n')
    
    with open(os.path.join(root, "pipeline_logs", "runtime_log.txt"), "a") as f:
        f.write(f"Round {gp} completed in {elapsed_time:.2f} seconds at {time.strftime('%H:%M:%S')}\n")
    
    return report

def process_season(season, rounds = None):
    
    project_root = r'C:\Users\ASUS\Desktop\F1 Predictions & Visualizations\F1-ML-Project'
    root = os.path.join(project_root, 'data')       
    
    schedule = fastf1.get_event_schedule(season)
    rlist = rounds if rounds is not None else schedule['RoundNumber'].tolist()
    
    for i,gp in enumerate(rlist,start = 1):
        print(f'\n========[{i}/{len(rlist)}] Processing round {gp}========')
        try:
            print('Processing..', gp)
            process_round(season, gp)
            time.sleep(1)
        except Exception as e:
            print('Failed round - ', gp, e)
            error_path = os.path.join(root,'pipeline_logs',f'round_{gp}_error.json')
            with open(error_path, 'w') as f:
                json.dump({
                    'round':gp,
                    'error':str(e)
                }, f)
                continue
            
    
    clean_files = sorted(glob.glob(os.path.join(root,'clean',f'season_{season}_round_*_clean.csv')))
    dfs = [pd.read_csv(p) for p in clean_files]
    
    if dfs:
        season_df = pd.concat(dfs, ignore_index=True)
        os.makedirs(os.path.join(root,'features'),exist_ok=True)
        season_df.to_csv(os.path.join(root,'features',f'season_{season}_lap_features.csv'), index = False)
        
        print('Season master saved.')
    else:
        print("No clean files found")
    
    
    
    