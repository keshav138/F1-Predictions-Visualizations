import fastf1
import pandas as pd


'''
    Loads the session object and returns it
'''
def load_session(season, gp, session_name = 'Race', telemetry = True, weather = True ):
    session = fastf1.get_session(season, gp, session_name)
    session.load(telemetry = telemetry, weather = weather)
    return session
    
    
    
'''
    Extracts rows from given season and gp, includes metadata , telemetry and details per lap
'''    
    
def extract_rows_from_session(session, season ,gp):
    
    meta = {}

    meta['season'] = season
    meta['gp'] = gp
    meta['race_name'] = session.event.get('EventName', session.event.get('Event'))
    meta['race_date'] = session.event.get('EventDate', None)
    meta['session'] = 'Race'
    meta['circuit_name'] = getattr(session,'track_name',None) or session.event.get('Circuit', None)
    meta['laps_total_in_race'] = int(session.laps['LapNumber'].max())

    rows = []

    for idx in session.laps.index:

        lap = session.laps.loc[idx]

        row = {}
        
        row['season'] = season
        row['round'] = gp
        row['session'] = session.name

        row['driver_name'] = lap.Driver
        row['driver_number'] = lap.DriverNumber
        row['team'] = lap.Team
        
        row['lap_number'] = lap.LapNumber
        row['lap_time'] = lap.LapTime.total_seconds() if pd.notna(lap.LapTime) else None

        row['sector1_time'] = lap.Sector1Time.total_seconds() if pd.notna(lap.Sector1Time) else None
        row['sector2_time'] = lap.Sector2Time.total_seconds() if pd.notna(lap.Sector2Time) else None
        row['sector3_time'] = lap.Sector3Time.total_seconds() if pd.notna(lap.Sector3Time) else None

        row['is_outlap'] = pd.notna(lap.PitOutTime)
        row['is_inlap'] = pd.notna(lap.PitInTime)
        
        row['position'] = lap.Position
        row['gap_to_leader'] = None
        row['speed_trap'] = lap.SpeedST if pd.notna(lap.SpeedST) else lap.SpeedI2

        row['compound'] = lap.Compound
        row['tyre_age'] = lap.TyreLife
        row['stint_number'] = lap.Stint

        try:
            w = lap.get_weather_data()
            if w is not None:
                row['air_temp'] = float(w.get('AirTemp', None))
                row['track_temp'] = float(w.get('TrackTemp', None))
                row['humidity'] = float(w.get('Humidity', None))
                row['pressure'] = float(w.get('Pressure', None))
                row['rainfall'] = bool(w.get('Rainfall', False))
                
                row['wind_speed'] = float(w.get('WindSpeed', None))
                row['wind_direction'] = float(w.get('WindDirection', None))
                row['has_weather'] = True
        except:
            row['air_temp'] = row['track_temp'] = row['humidity'] = None
            row['pressure'] = row['wind_speed'] = row['wind_direction'] = None
            row['rainfall'] = False
            row['has_weather'] = False
        
        
        
        try:
            tel = lap.get_telemetry()
            
            if tel is not None:
                row['has_telemetry'] = True
            
            row['avg_speed'] = tel['Speed'].mean() if 'Speed' in tel.columns else None
            row['max_speed'] = tel['Speed'].max() if 'Speed' in tel.columns else None

            row['avg_throttle'] = tel['Throttle'].mean() if 'Throttle' in tel.columns else None
            row['std_throttle'] = tel['Throttle'].std() if 'Throttle' in tel.columns else None

            row['avg_brake'] = tel['Brake'].mean() if 'Brake' in tel.columns else None
            row['std_brake'] = tel['Brake'].std() if 'Brake' in tel.columns else None

            row['max_rpm'] = tel['RPM'].max() if 'RPM' in tel.columns else None

            row['avg_gear'] = tel['nGear'].mean() if 'nGear' in tel.columns else None
            del tel

        except Exception as e:
            
            print(f'Telemetry error for lap idx {idx} (driver {lap.Driver}): {e}')
            
            row['has_telemetry'] = False
            row['avg_speed'] = None  
            row['max_speed'] = None 

            row['avg_throttle'] = None
            row['std_throttle'] = None

            row['avg_brake'] = None
            row['std_brake'] = None

            row['max_rpm'] = None

            row['avg_gear'] = None
            
        row.update(meta)
        rows.append(row) 
    
    df_raw = pd.DataFrame(rows)
    return rows, df_raw
        