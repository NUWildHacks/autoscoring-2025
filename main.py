from autoscorer.spreadsheet_reader import SpreadsheetReader
from autoscorer.round1_query import Round1Query
from autoscorer.round2_query import Round2Query


def fix_cols(df, colname):
    df[colname] = df[colname].fillna(0).astype(float)
    return df


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
    round1 = round1("simulated_scores_2.csv", True)
    # round2 = round2("sample_round2_data.csv", round1, True)
