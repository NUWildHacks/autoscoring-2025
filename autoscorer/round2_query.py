"""
Round 2
"""

import duckdb as quack
import pandas as pd

class Round2Query:
    def __init__(self, df, round1_df):
        self.df = df
        self.round1_df = round1_df
    
    def query(self):
        scores = {}

        for team in self.df["team_1st"]:
            if team in scores:
                scores[team] += 5
                continue
            scores[team] = 5

        for team in self.df["team_2nd"]:
            if team in scores:
                scores[team] += 3
                continue
            scores[team] = 3

        for team in self.df["team_3rd"]:
            if team in scores:
                scores[team] += 3
                continue
            scores[team] = 3
        
        scores_df = pd.DataFrame(sorted(scores.items()), columns=['project_id', 'final_round_points'])
        round1_df = self.round1_df

        result = quack.sql("""
SELECT
    round1_df.project_id, team_name, final_round_points, sum_1hl, sum_mean, random() AS random
FROM
    scores_df INNER JOIN round1_df ON scores_df.project_id = round1_df.project_id
ORDER BY final_round_points DESC, sum_1hl DESC, sum_mean DESC, random DESC;
        """)

        print(result)


