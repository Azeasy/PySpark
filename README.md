# PySpark
### Pre-requisites
You'll need either python3 or Spark downloaded from [here](https://spark.apache.org/downloads.html) or on Docker

Make sure to create env.py file in the base directory with the following data:
```
API_KEY = 'your_api_key_for_opencagedata'
BASE_URL = 'https://api.opencagedata.com/geocode/v1/json'
BASE_DIR = '/path/to/your/working/folder/'
```
Create two folders: `restaurant_csv` and `weather`

Download input data to the folders, sample data with the correct schema could be found in `tests/fixtures.py`

### Run with Python
In the terminal:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
If you are using windows, you'll need to run `activate.bat` file in the `venv/Scripts/` folder

Then simply run
```
python etl_job.py
```

### Run with Spark
Submit the job to the spark master:
```
spark-submit \                                         
  --master local \
  --deploy-mode client \
  /Users/azeasy/Desktop/EPAM/task_2_app/etl_job.py \
  8
```

The result partitioned by date (year, month, day) should appear in the base directory under the `output/` folder



#### Fun fact
    I've only found one row with null lat or lng,
    and there exists this same city in the database
    but the task is to make API request to fill in lat and lng,
    so I'll use this record to compare our data to API

So I printed the difference to see how accurate coordinates API was:

    target_df = final_df.filter(col("city") == 'Dillon')
    target_df.show()


The result (The second line where `franchise_name` is Savoria was with None `lat` and `lng`):
```
+-----------+------------+---------------+-----------------------+-------+------+----------+-----------+
|         id|franchise_id| franchise_name|restaurant_franchise_id|country|  city|       lat|        lng|
+-----------+------------+---------------+-----------------------+-------+------+----------+-----------+
|60129542152|           9|The Grill House|                  71555|     US|Dillon|    34.436|    -79.370|
|85899345920|           1|        Savoria|                  18952|     US|Dillon|34.4014089|-79.3864339|
+-----------+------------+---------------+-----------------------+-------+------+----------+-----------+
```
