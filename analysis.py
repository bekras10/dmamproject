import sqlite3
import pandas as pd
import numpy as np
import os

def main():

    # Connect to and query db

    conn = sqlite3.connect(f'data/soccer_stats.db')

    df = pd.read_sql('''
        WITH RankedMatches AS (
            SELECT 
                player_id, 
                player_name, 
                match_date, 
                xg, 
                xa,
                ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC) as match_rank
            FROM player_match_stats
        )
        SELECT player_id, player_name, match_date, xg, xa
        FROM RankedMatches
        WHERE match_rank <= 5;
    ''', conn)

    if df.empty:
        print(f"Abort: No match data found in.")
        conn.close()
        return
    
    results = []
    
    # Iterate through grouped subsets directly in the main function

    for player_id, group in df.groupby('player_id'):
        w = [0.40, 0.25, 0.15, 0.12, 0.08]
        weights = w[:len(group)]
        weights = [val / sum(weights) for val in weights]
        
        form_metric = np.average(group['xg'] + group['xa'], weights=weights)
        
        results.append({
            'player_id': player_id,
            'player_name': group['player_name'].iloc[0],
            'form_factor': round(form_metric, 3)
        })

    # Convert results back to a DataFrame and sort

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(by='form_factor', ascending=False)

    try:
        conn.close()
        results_df.to_csv('data/form_factor.csv', index=False)
    except PermissionError:
        print(f"Error: is currently locked by another program. Close it and retry.")

if __name__ == '__main__':
    main()