"""
Round 1
"""

import duckdb as quack

class Round1Query:
    def __init__(self, df, lambda_judge=5, lambda_project=20):
        self.df = df
        self.lambda_judge = lambda_judge
        self.lambda_project = lambda_project
    
    def query(self):
        data = self.df

        data = quack.sql("""
SELECT
    judge_name,
    project_id,
    team_name,
    technical_complexity,
    usefulness,
    originality,
    design,
    presentation
FROM data
        """)

        global_average = quack.sql("""
SELECT
    ROUND(
        AVG((3*technical_complexity + 3*usefulness + 2*originality + 1.5*design + 0.5*presentation) * 0.5),
        2
    ) AS average
FROM data
        """).fetchone()[0]

        score_by_judge = quack.sql(f"""
SELECT
    judge_name,
    COUNT(*) AS count,
    (count/(count + ?)) AS weight,
    ROUND(
        AVG((3*technical_complexity + 3*usefulness + 2*originality + 1.5*design + 0.5*presentation) * 0.5),
        2
    ) AS average,
    ROUND(average - ?, 2) AS delta
FROM data GROUP BY judge_name
        """, params=[self.lambda_judge, global_average])

        result1 = quack.sql("""
SELECT
    data.project_id AS project_id,
    data.judge_name AS judge_name,
    data.team_name AS team_name,
    0.5*(3*data.technical_complexity + 3*data.usefulness + 2*data.originality + 1.5*data.design + 0.5*data.presentation) AS sum,
    score_by_judge.weight AS weight,
    score_by_judge.delta AS delta,
    (sum - ?) AS project_shift
FROM data INNER JOIN score_by_judge ON data.judge_name = score_by_judge.judge_name
ORDER BY project_id;
        """, params=[global_average])

        result2 = quack.sql("""
SELECT
    project_id,
    (COUNT(*)/(COUNT(*) + ?)) AS eval_count,
    ROUND(MEDIAN(sum), 3) AS sum_1hl,
    ROUND(SUM(sum), 3) AS total,
    ROUND(MEAN(sum), 3) AS sum_avg,
    ROUND(total - SUM(weight * delta), 3) AS total_judge_shifted_weighted,
    ROUND(total_judge_shifted_weighted + (SUM(project_shift) * eval_count), 3) AS project_shift_weighted,
    --begin unused
        --ROUND(total - SUM(delta), 3) AS total_judge_shifted_unweighted,
        --ROUND(total_judge_shifted_unweighted + SUM(project_shift), 3) AS project_shift_unweighted,
    --end unused
    random() AS random
FROM result1
GROUP BY project_id
ORDER BY project_shift_weighted, sum_1hl, sum_avg, random;
        """, params=[self.lambda_project])

        result3 = quack.sql("""
WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY result2.project_id ORDER BY result2.project_id) AS rn
    FROM result2
    INNER JOIN result1 ON result2.project_id = result1.project_id
)
SELECT
    project_id,
    team_name,
    project_shift_weighted, --total_judge_shifted_weighted, total_judge_shifted_unweighted, project_shift_unweighted,
    sum_1hl, sum_avg, random
FROM cte
WHERE rn = 1
ORDER BY project_shift_weighted DESC, sum_1hl DESC, sum_avg DESC, random DESC;
        """)
        
        return result3
