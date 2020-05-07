import json
import sqlalchemy as sql
import pandas as pd

# reads log file

def extracting_log():
    dflist = []

    with open('data/test_runs.log') as f:
        lines = f.readlines()

        for line in lines:
            line = json.loads(line)
            if "out of" in line['result']:
                a = line['result'].split("out of")
                line['received']= a[0].strip()
                line['total'] = a[1].strip()
            elif "/" in line['result']:
                a = line['result'].split("/")
                line['received'] = a[0].strip()
                line['total'] = a[1].strip()
            print(line)
            dflist.append(line)


    return pd.DataFrame(dflist)

def loading_df_in_DB(mydf):
    less1 = pd.merge(dflessons,mydf, how= 'left', left_on= ['id','title'], right_on=['lesson_id','lesson_title'], suffixes=("_dflessons","_mydf"))
    print(less1)

    less1 = less1[['result','student_id','id','title','received','total']]
    less1.to_sql('New_data', engine, index=False)


if __name__ == "__main__":
    server = 'data/.internal.db'

    myQuery = 'select * from lessons'
    engine = sql.create_engine('sqlite:///' + server)
    dfless = pd.read_sql_query(myQuery, engine)

    dflessons = pd.DataFrame(dfless)
    mydf = extracting_log()
    loading_df_in_DB(mydf)