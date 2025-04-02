"""
Assign judges
"""

import random

import duckdb as quack

class AssignJudgeQuery:
    def __init__(self, df_judges, df_projects):
        self.df_judges = df_judges
        self.df_projects = df_projects
    
    def query(self):
        judges = self.df_judges
        projects = self.df_projects

        count = 0

        while True:
            random_judges = quack.sql("""
SELECT
    *
FROM judges ORDER BY random()
            """)
            project_mapped_judges = quack.sql("""
SELECT 
    p.project_name,
    p.project_url,
    j.judge_name,
    ROW_NUMBER() OVER (PARTITION BY p.project_name ORDER BY random()) AS rn
FROM projects p
CROSS JOIN random_judges j
            """)

            result1 = quack.sql("""
SELECT project_name, project_url, judge_name
FROM project_mapped_judges
WHERE rn <= 3
            """)

            result2a = quack.sql("""
WITH cte AS (
    SELECT
        project_name,
        judge_name,
        ROW_NUMBER() OVER (PARTITION BY project_name ORDER BY judge_name) AS rn
    FROM result1
)
SELECT
    project_name,
    MAX(CASE WHEN rn = 1 THEN judge_name END) AS judge_1,
    MAX(CASE WHEN rn = 2 THEN judge_name END) AS judge_2,
    MAX(CASE WHEN rn = 3 THEN judge_name END) AS judge_3
FROM cte
GROUP BY project_name
ORDER BY project_name ASC
            """)

            result2b = quack.sql("""
SELECT judge_name, count(*) AS count FROM result1 GROUP BY judge_name
            """)

            average_count = quack.sql("""
SELECT 
    MEAN(count)
FROM result2b
            """).fetchone()[0]
            
            abs_count = quack.sql("""
SELECT
    judge_name,
    count,
    ABS(count - ?) AS abs_val
FROM result2b
ORDER BY abs_val ASC
            """, params=[average_count])

            judge_count_delta = quack.sql("""
SELECT
    MEAN(abs_val),
    MAX(abs_val)
FROM abs_count
            """).fetchone()

            average_ok = judge_count_delta[0] < 1
            max_ok = judge_count_delta[1] < 2
            count += 1
            if count % 1000 == 0:
                print(f"got to {count} with no result :)")
            if average_ok and max_ok:
                print(f"used {count} iterations")
                break

        return result1
