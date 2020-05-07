import requests

r = requests.get('http://localhost:8000/batches/2019-08-22%2019:22:40')

r.status_code

output = r.text 
with open ("test_runs.log","w") as f:
	f.write(output)
	
	

