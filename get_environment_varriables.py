import os

os.environ["envn"] = "DEV"
os.environ["header"] = "True"
os.environ["inferSchema"] = "True"

header = os.environ["header"]
inferSchema = os.environ['inferSchema']
envn = os.environ["envn"]

appname = "practise pyspark"

current_path = os.getcwd()

source_olap = os.path.join(current_path, "Source", "olap")
source_oltp = os.path.join(current_path, "Source", "oltp")