#import necessary modules
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

def graphdf():
    #Set environment variables for py4jjava error
    import os
    import sys
    os.environ['PYSPARK_PYTHON']= sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']= sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "notebook"
    # Create a SparkSession object
    spark= SparkSession.builder.appName("Amazon_CoPurchase_Dataset_graph").getOrCreate()
    #Read in the data from a .txt.gz file and create a DataFrame
    df1 = spark.read.option('comment', '#').csv(r"C:\Users\vrushalideshmukh\Documents\BFL_Internship_Docs\Amazon_ungraph.txt")
    # df1.describe()
    # Read in the data from a .txt file and create a DataFrame
    metadf = spark.read.options().text(r"C:\Users\vrushalideshmukh\Documents\BFL_Internship_Docs\amazon-meta.txt")
    metadf.show(10,truncate=False)
    # This code splits the '_c0' column by tab delimiter, and creates two new columns 'From' and 'To'
    # 'From' column contains the first part of the split and 'To' column contains the second part of the split
    # The new DataFrame with these two columns is stored in 'df1'
    df1 = df1.withColumn('From', split(df1['_c0'], '\t').getItem(0).cast('int')).withColumn('To', split(df1['_c0'], '\t').getItem(1).cast('int'))
    # This code drops the '_c0' column from 'df1' and stores the resulting DataFrame in 'df1'
    df1 = df1.drop('_c0')
    #This code displays the content of 'df1' DataFrame
    df1.show()
    # Using ungraph dataset to get the nodes and the edges of each id
    df1= df1.groupBy(col('From')).agg(collect_list(col('To'))).orderBy(col('From').asc())
    df1.show()
    from .Metadataload import metadataload
    metadf= metadataload()
    from .revdataload import reviewsdataload
    reviewdf= reviewsdataload()
    joined_df = metadf.join(df1, metadf.ID == df1["From"],'right')
    joined_df= joined_df.drop("From").select("ID", "collect_list(To)", "asins", "titles", "groups", "salesranks", "total_reviews", "avg_rating", "similars_list", "categories_list")
    joined_df= joined_df.withColumnRenamed("collect_list(To)", "To_List")
    joined_df = joined_df.limit(1500)
    joined_df = joined_df.select('ID', 'To_List', 'asins', 'titles', 'groups', 'salesranks','total_reviews', 'avg_rating', explode('similars_list').alias('similars'),'categories_list')
    joined_df = joined_df.select('ID', 'To_List', 'asins', 'titles', 'groups', 'salesranks','total_reviews', 'avg_rating', explode('categories_list').alias('category'),'similars')
    joined_df = joined_df.select('ID', 'asins', 'titles', 'groups', 'salesranks','total_reviews', 'avg_rating', explode('To_List').alias('to'),'similars', 'category')
    import networkx as nx
    import matplotlib.pyplot as plt
    # Create an empty graph
    G = nx.Graph()

    # Convert PySpark dataframe to Pandas dataframe
    pandas_df = joined_df.toPandas()
    # Add nodes with attributes from Pandas dataframe
    for i, row in pandas_df.iterrows():
        if row['asins'] is not None:  # check if asins is not None 
            # Add node 'ASIN' with attributes
            G.add_node(row['asins'], asins=row['asins'], id=row['ID'], titles=row['titles'], groups=row['groups'], salesranks=row['salesranks'], avg_rating=row['avg_rating'], category = row['category'])

            # Add node 'GROUP' with attributes
            G.add_node(row['groups'], groups=row['groups'])

            # Add node 'CATEGORIES' with attributes
            category = row['category']
            for c in category:
                G.add_node(c, category=c)

    # Add edges between nodes
    for i, row in pandas_df.iterrows():
        # Add edges from 'ASIN' to 'GROUP' and 'CATEGORIES'
        if row['asins'] is not None:
            G.add_edge(row['asins'], row['groups'], label='has_group_edge')
            category = row['category']
            for c in category:
                G.add_edge(row['asins'], category, label='belongs_to_edge')

            # Add edges from 'ASIN' to 'ASIN' and 'CATEGORIES'
            similars = row['similars']
            for s in similars:
                G.add_edge(row['asins'], s, label='is_similar_to_edge')

            to = str(row['to'])
            for t in to:
                G.add_edge(str(row['ID']), str(t), label='co_purchase_edge')

            # Add edges from 'GROUP' to 'CATEGORIES'
            G.add_edge(row['groups'], category, label='falls_under_edge')
    from pyspark.sql.functions import col, collect_list, lit, desc

    def getRecomm(inpasin, inpgroup, inpcategory, topN=None):
        if topN is None:
            N = 5
        else:
            N = topN
        
        # Define traversal paths for different inputs
        if inpcategory is not None:
            if inpgroup is not None:
                if inpasin is not None:
                    # Traversal through all edges
                    df = joined_df.filter(col('category').contains(inpcategory))
                    df = df.filter(col('groups')==inpgroup)
                    df = df.filter(col('asins')!=inpasin)
                else:
                    df = joined_df.filter(col('category').contains(inpcategory))
                    df = df.filter(col('groups')==inpgroup)
            else:
                if inpasin is not None:
                    df = joined_df.filter(col('category').contains(inpcategory))
                    df = df.filter(col('asins')!=inpasin)
                else:
                    df = joined_df.filter(col('category').contains(inpcategory))
        else:
            if inpgroup is not None:
                if inpasin is not None:
                    df = G.bfs(fromExpr=f"id = '{inpasin}'", toExpr=f"group = '{inpgroup}'", edgeFilter="edge_type = 'co_purchase_edge'")
                    df = df.union(joined_df.filter(col('groups')==inpgroup))
                    df = df.filter(col('category').contains(inpcategory))
                    df = df.filter(col('asins')!=inpasin)
                else:
                    df = joined_df.filter(col('groups')==inpgroup)
                    df = df.filter(col('category').contains(inpcategory))
            else:
                if inpasin is not None:
                    # Traversal through ASIN nodes using co_purchase_edge, has_group_edge, has_category_edge, falls_under_edge, is_similar_to_edge
                    df = G.bfs(fromExpr=f"id = '{inpasin}'", edgeFilter="edge_type in ('co_purchase_edge', 'has_group_edge', 'has_category_edge', 'falls_under_edge', 'is_similar_to_edge')")
                    # Filter df by group corresponding to the inpasin
                    group = joined_df.filter(col('asins')==inpasin).select('groups').collect()[0][0]
                    df = df.filter(col('groups')==group)
                    df = df.filter(col('category').contains(inpcategory))
                    df = df.filter(col('asins')!=inpasin)
                else:
                    df = joined_df
                    df = df.filter(col('category').contains(inpcategory))
        
        # Calculate scores for all products in the filtered df
        df = df.withColumn('score', ((10000000-col('salesranks'))/10000000 + col('total_reviews')/100 + col('avg_rating'))/3)
        
        df = df.select('asins', 'titles').distinct().sort(asc('score')).limit(N)
    return df

