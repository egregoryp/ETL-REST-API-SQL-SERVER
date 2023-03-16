
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'
        self.process2 = 'qwerty'


    def enunciado1(self, df_survey_flat):
        q = """
                SELECT
                    *
                FROM
                    df_survey_flat
            """
        result = sqldf(q)

        return result