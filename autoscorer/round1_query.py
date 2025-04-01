"""
Round 1
"""

import duckdb as quack

class Round1Query:
    def __init__(self, df):
        self.df = df
    
    def query(self):
        data = self.df

        data = quack.sql("""
SELECT
    upper(project_id),
    *
FROM data
        """)

        result1 = quack.sql("""
SELECT
    project_id,
    team_name,
    3*technical_complexity + 3*usefulness + 2*originality + 1.5*design + 0.5*presentation AS sum
FROM data;
        """)

        result2 = quack.sql("""
SELECT
    project_id,
    MEDIAN(sum) AS sum_1hl,
    MEAN(sum) AS sum_mean,
    random() AS random
FROM result1
GROUP BY project_id
ORDER BY sum_1hl, sum_mean, random;
        """)

        result3 = quack.sql("""
WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY result2.project_id ORDER BY result2.project_id) AS rn
    FROM result2
    INNER JOIN result1 ON result2.project_id = result1.project_id
)
SELECT project_id, team_name, sum_1hl, sum_mean, random
FROM cte
WHERE rn = 1
ORDER BY sum_1hl DESC, sum_mean DESC, random DESC;
        """)
        
        return result3
