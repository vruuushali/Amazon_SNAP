from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def metadataload():
    #Set environment variables for py4jjava error
    import os
    import sys
    os.environ['PYSPARK_PYTHON']= sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']= sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "notebook"
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
    metadf = spark.createDataFrame(dict[:1500],['value'])

    # import pyspark.sql.functions module and alias it as F
    import pyspark.sql.functions as F

    # Define regular expressions as string literals
    product_id_regex = '^Id:\s*(\d+)'
    product_ASIN_regex = '.*(?:ASIN:\s*)(\d+|\w+)'
    product_title_regex = 'title:\s*(.*)'
    product_group_regex = '.*(?:group:\s*)([\w]*)'
    sales_rank_regex = '.*(?:salesrank:\s*)(\d*)'
    similar_products_regex = '.*(?:similar:\s*[1-9]\s*)([\w ?]*)'
    categories_regex = 'categories:\s*[1-9]\s*([\s\S]*?)(?=reviews|\Z)'
    # reviews_regex = '.*(?:reviews:\\s*)(\\d+),(\\d+),([\\d.]+),(.*)'
    total_reviews_regex = '.*(?:reviews:\s*)total:\s*(\d+)'
    avg_rating_regex = '.*avg rating:\s*([\d.]+)'
    # custdata_regex = '\n*\r*\s*(\d*-\d*-\d*)\s*cutomer:\s*([A-Za-z0-9]*)\s*rating:\s*(\d*)\s*votes:\s*(\d*)\s*helpful:\s*(\d*)\r*\n*'

    # list of metadata field names
    metadata_fields = ['id', 'asins', 'titles', 'groups', 'salesranks', 'categories', 'similars','total_reviews', 'avg_rating']

    # loop through each regular expression and field name
    for i, regex in enumerate([product_id_regex, product_ASIN_regex, product_title_regex, product_group_regex, sales_rank_regex, categories_regex, similar_products_regex, total_reviews_regex, avg_rating_regex]):
        # get the corresponding field name from the metadata_fields list
        field_name = metadata_fields[i]
        # apply regular expression and create a new column with the field name
        metadf = metadf.withColumn(field_name, F.regexp_extract('value', regex, 1))

    # Split the 'similars' column by whitespace and convert to list
    split_col = F.split(metadf['similars'], '\\s+')

    # Define a user-defined function (UDF) to convert the split column to a list
    # This lambda function simply takes a single input argument x and returns it unchanged.
    # The purpose of the UDF is to apply a transformation to the input data, and the transformation is defined by the lambda function.
    # The UDF is being used to convert the split column (which is a column of strings) to a list. 
    # The lambda function simply returns the input string unchanged, so the UDF applies no transformation to the data. 
    # However, because we have specified that the return type of the UDF is ArrayType(StringType())
    # Hence, the resulting output column will be a column of lists of strings.
    to_list_udf = F.udf(lambda x: x, ArrayType(StringType()))

    # Apply the UDF to the split column and create a new column with the resulting list
    metadf = metadf.withColumn('similars_list', to_list_udf(split_col))

    #The resulting 'similars_list' column will contain a list of strings that were previously separated by whitespace in the 'similars' column.
    # Drop the original 'similars' column since we no longer need it
    metadf=metadf.drop('similars')

    # Split categories by '\n' and create new columns to separate categories
    metadf = metadf.withColumn('categories_list', F.split(metadf['categories'], '\n'))
    metadf = metadf.withColumn('category_1', metadf['categories_list'][0])
    metadf = metadf.withColumn('category_2', metadf['categories_list'][1])
    metadf = metadf.withColumn('category_3', metadf['categories_list'][2])
    metadf = metadf.withColumn('category_4', metadf['categories_list'][3])
    metadf = metadf.withColumn('category_5', metadf['categories_list'][4])

    # Use a conditional statement to check if there is a 6th category, and create a new column for it if there is
    metadf = metadf.withColumn('category_6', F.when(F.size(metadf['categories_list']) > 1, metadf['categories_list'][5]).otherwise(None))

    # Drop the 'categories_list' column since we no longer need it
    # metadf = metadf.drop('categories_list')

    # Drop the original 'categories' column since we've extracted the relevant information into new columns
    metadf=metadf.drop('categories')

    # Drop the 'value' column since it was only used temporarily to extract the metadata fields

    metadf=metadf.drop('value')

    # Using select casting the string columns as the required data type

    metadf=  metadf.withColumn("ID",col("id").cast("int"))
    metadf = metadf.withColumn("salesranks", col("salesranks").cast("double"))
    metadf = metadf.withColumn("avg_rating", col("avg_rating").cast("float"))
    metadf = metadf.withColumn("total_reviews", col("total_reviews").cast("int"))

    return metadf



