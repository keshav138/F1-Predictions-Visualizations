from fastf1 import Cache

import os

CACHE_DIR = r'C:\Users\ASUS\Desktop\F1 Predictions & Visualizations\F1-ML-Project\data\cache'
os.makedirs(CACHE_DIR,exist_ok=True)

Cache.enable_cache(CACHE_DIR)