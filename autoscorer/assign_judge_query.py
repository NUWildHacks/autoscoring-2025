"""
Assign judges
"""

import duckdb as quack

class AssignJudgeQuery:
    def __init__(self, df_judges, df_projects):
        self.df_judges = df_judges
        self.df_projects = df_projects
    
    def query(self):
        judges = self.df_judges
        projects = self.df_projects
        
        return quack.sql("SELECT * FROM judges")
