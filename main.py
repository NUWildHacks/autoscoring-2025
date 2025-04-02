from autoscorer.spreadsheet_reader import SpreadsheetReader
from autoscorer.round1_query import Round1Query
from autoscorer.round2_query import Round2Query
from autoscorer.assign_judge_query import AssignJudgeQuery


def fix_cols(df, colname):
    df[colname] = df[colname].fillna(0).astype(float)
    return df


def assign_judges(path_judges: str, path_projects: str, p=True):
    reader_judges = SpreadsheetReader(path_judges)
    reader_judges.replace_columns([
        "judge_name"
    ])
    reader_projects = SpreadsheetReader(path_projects)
    reader_projects.replace_columns([
        "project_name", "project_url"
    ])

    judges_df = reader_judges.get_df()
    projects_df = reader_projects.get_df()

    result = AssignJudgeQuery(judges_df, projects_df).query()
    if p:
        print(result)
    return result


def round1(path: str, p=True):
    reader = SpreadsheetReader(path)
    reader.replace_columns([
        "timestamp","judge_name","project_id","team_name",
        "technical_complexity",
        "usefulness",
        "originality",
        "design",
        "presentation",
        "comments",
    ])

    round1_df = reader.get_df()
    round1_df = fix_cols(round1_df, "technical_complexity")
    round1_df = fix_cols(round1_df, "usefulness")
    round1_df = fix_cols(round1_df, "originality")
    round1_df = fix_cols(round1_df, "design")
    round1_df = fix_cols(round1_df, "presentation")

    result = Round1Query(round1_df).query()
    if p:
        print(result)
    return result


def round2(path: str, r1, p=True):
    reader = SpreadsheetReader(path)
    reader.replace_columns([
        "timestamp","judge_name","comments",
        "team_1st",
        "team_2nd",
        "team_3rd",
        "rfd"
    ])

    round2_df = reader.get_df()

    result = Round2Query(round2_df, r1).query()
    if p:
        print(result)
    return result


if __name__ == "__main__":
    judges = assign_judges("judge_list.csv", "project_list.csv", True)
    # round1 = round1("simulated_scores_2.csv", True)
    # round2 = round2("sample_round2_data.csv", round1, True)
