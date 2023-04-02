from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def reviewsdataload():
    #Set environment variables for py4jjava error
    import os
    import sys
    os.environ['PYSPARK_PYTHON']= sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']= sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "notebook"
    # Create a SparkSession object

    spark= SparkSession.builder.appName("Amazon_get_recomm").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "10")
    spark.conf.set("spark.sql.execution.arrow.timeout", "300")
    spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
    # Read in the data from a .txt file and create a DataFrame
    # metadf = spark.read.options().text(r"C:\Users\vrushalideshmukh\Documents\BFL_Internship_Docs\amazon-meta.txt")
    # Create an empty list called 'listing'
    listing = []
    #Open the file at the specified path and encode it using utf-8
    # fileopen = open(r"C:\Users\vrushalideshmukh\Documents\BFL_Internship_Docs\amazon-meta.txt",encoding='utf-8')
    fileopen = open(r"C:\Users\vrushalideshmukh\Documents\BFL_Internship_Docs\amazon-meta.txt",encoding='utf-8')
    #Start an infinite loop until there are no more lines to read in the file
    while True:
        line = fileopen.readline()
        if not line:
            break
        else: listing.append(line)

    import re
    # create an empty list
    dict=[]
    # initialize flag to 0
    flag = 0
    # initialize newstr variable to empty string
    newstr = ""

    # loop through each line in the listing
    for i in listing:
        # check if the line contains "Id"
        if "Id" in i:
            # check if flag is 0
            if flag == 0:
                newstr= "" + i
                flag = 1
                continue
            # check if flag is 1
            elif flag == 1:
                # remove trailing newline character
                if newstr.endswith('\n'):
                    newstr = newstr[:-1]
                dict.append([newstr])
                newstr=""
        newstr += i

    newdict=[]
    newdict= dict
    # create a spark dataframe from first 1500 tuples
    reviewdf= spark.createDataFrame(newdict[:50],['value'])

    # import pyspark.sql.functions module and alias it as F
    import pyspark.sql.functions as F
        
    custdata_regex= "\n*\r*\s*(\d*-\d*-\d*)\s*cutomer:\s*([A-Za-z0-9]*)\s*rating:\s*(\d*)\s*votes:\s*(\d*)\s*helpful:\s*(\d*)\r*\n*"

    import re
    import pyspark.sql.functions as F

    # dictionary to store extracted customer data
    customer_data = {}

    # loop through each row in the dataframe
    for row in reviewdf.collect():
        
        # extract Id value
        Id = (re.findall('^Id:\s*(\d+)', row[0])[0])
        
        # extract customer data
        matches = re.findall(custdata_regex, row[0])
        
        # loop through each match and store in dictionary
        for match in matches:
            date = match[0]
            customer = match[1]
            rating = int(match[2])
            votes = int(match[3])
            helpful = int(match[4])
            
            if Id not in customer_data:
                customer_data[Id] = []
            
            customer_data[Id].append({'date': date, 'customer': customer, 'rating': rating, 'votes': votes, 'helpful': helpful})
            
    # create dataframe from dictionary
    customer_df = spark.createDataFrame([(k, v) for k, v in customer_data.items()], ['product_id', 'customer_data'])

    # join customer data with original dataframe
    reviewdf = reviewdf.withColumn("product_id", F.regexp_extract('value', '^Id:\s*(\d+)', 1))
    reviewdf = reviewdf.join(customer_df, 'product_id', 'left')

    # create separate columns for date, customer, rating, votes, and helpful
    reviewdf = reviewdf.withColumn("customer_data", F.explode("customer_data")) \
                    .select("product_id", "value", F.col("customer_data.date").alias("date"), \
                            F.col("customer_data.customer").alias("customer"), \
                            F.col("customer_data.rating").alias("rating"), \
                            F.col("customer_data.votes").alias("votes"), \
                            F.col("customer_data.helpful").alias("helpful"))

    reviewdf=  reviewdf.withColumn("id",col("product_id").cast("int"))

    reviewdf= reviewdf.drop("value")
    reviewdf= reviewdf.drop("product_id")

    return reviewdf

