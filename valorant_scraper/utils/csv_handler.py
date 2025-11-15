import pandas as pd
import os

def save_to_csv(data, filepath):
    """Save data to CSV, creating directories if needed"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    if os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
        print(f"Appended data to existing file: {filepath}")
    else:
        df.to_csv(filepath, index=False)
        print(f"Created new file and saved data: {filepath}")

def load_from_csv(filepath):
    """Load data from CSV"""
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} not found")
        return pd.DataFrame()
    
    return pd.read_csv(filepath)