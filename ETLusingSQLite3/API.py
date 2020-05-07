from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps

e = create_engine('sqlite:///students.db')

app = Flask(__name__)
api = Api(app)

class batches(Resource):
    def get(self):
        #Connect to databse
        conn = e.connect()
        #Perform query and return JSON data
        query = conn.execute("select distinct class_no from Student")
        list_class=[]
        result=[]
        for i in query.cursor.fetchall():
        	list_class.append(i[0])
        	query1=conn.execute('select id, name, lesson_no,result,created_at from Student where class_no='+str(i[0])+'')
        	result.append(dict({'class_data': [dict(zip(tuple(query1.keys()), j)) for j in query1.cursor],'class':i[0]}))
        return dict({'data' :result})
        
        
class Class_report(Resource):
    def get(self, Date_filter):
        conn = e.connect()
        query = conn.execute("select * from student where created_at between '%s' and datetime('now') limit 1000" % Date_filter)
        # Query the result and get cursor.Dumping that data to a JSON is looked by extension
        result = {'data': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return result

api.add_resource(Class_report, '/batches/<string:Date_filter>')
api.add_resource(batches, '/batches')



if __name__ == '__main__':
    app.run(host='localhost', port=8000)
