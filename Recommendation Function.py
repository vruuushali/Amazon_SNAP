#import necessary modules
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from Metadataload import metadataload
metadf= metadataload()
# metadf.show()

from pyspark.sql.functions import collect_set
def getTitle(id):
    title = metadf.filter(col('ID') == id).select('ID', 'asins', 'titles', 'salesranks', 'total_reviews', 'avg_rating', 'groups', explode('categories_list').alias('category'))
    # extract the set of unique titles
    title_set = title.select(collect_set('titles')).collect()[0][0]
    return title_set

inp_title= getTitle(26)

from pyspark.sql.functions import collect_set

id_title_df = metadf.select('ID', 'titles').distinct()
id_title_set = id_title_df.select(collect_set('titles')).collect()[0][0]

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# define the Jaccard similarity function
def jaccard_similarity(input_set, set_list):
    if not input_set or not set_list:
        return 0.0
    intersection_size = len(input_set.intersection(set_list[0]))
    union_size = len(input_set.union(set_list[0]))
    for s in set_list[1:]:
        intersection_size = intersection_size + len(input_set.intersection(s))
        union_size = union_size + len(input_set.union(s))
    return intersection_size / union_size


# define the threshold
threshold = 0.3

# define a UDF to apply the function to the dataframe
jaccard_similarity_udf = udf(lambda x: jaccard_similarity(set(inp_title), [set(y) for y in x]), FloatType())
# jaccard_similarity_udf = udf(lambda metadf: jaccard_similarity(inp_title, id_title_set), FloatType())

# apply the function to the dataframe and filter by the threshold
matches = metadf.filter(jaccard_similarity_udf(metadf['titles']) >= threshold)
matches = matches.select('ID', 'asins', 'titles', 'groups', 'total_reviews', 'avg_rating', 'salesranks', explode('categories_list').alias('category'))


cat_df= metadf.select('ID', 'asins', 'titles','groups','avg_rating','salesranks','total_reviews', F.explode('categories_list').alias('category')).distinct()
cat_df = cat_df.withColumnRenamed("category", "cat_category")
def getRelevantCategory(id, group):
    if group is None:
        categories_context_df = metadf.filter(col('ID') == id).select('ID', 'asins', 'titles', 'salesranks', 'total_reviews', 'avg_rating', 'groups', explode('categories_list').alias('category'))
        relevant_category_df = categories_context_df
    else:
        similars_context_df = metadf.filter(col('ID') == id).select('ID', 'asins', 'titles', 'salesranks', 'total_reviews', 'avg_rating', 'groups', 'categories_list', explode('similars_list').alias('similar_asins'))
        matches_df = similars_context_df.filter(col('similar_asins').isin(metadf.select('asins').rdd.flatMap(lambda x: x).collect()))
        out_categories_context_df = matches_df.select('ID', 'asins', 'titles', 'groups', 'total_reviews', 'avg_rating', 'salesranks', explode('categories_list').alias('category'))
        out_categories_context_df = out_categories_context_df.filter((col('groups') == group))
        relevant_category_df = out_categories_context_df.distinct()
    return relevant_category_df

def getRecommendation(id, group, categories, top_n):
    if id is None:
        print("Invalid input: id cannot be null")
        return

    # get relevant categories
    sorted_categories = getRelevantCategory(id,group)
    # filter the categories in sorted_categories that match with those in cat_df
    filtered_categories = sorted_categories.filter(col('category').isin(cat_df.select('cat_category').rdd.flatMap(lambda x: x).collect())).distinct()
    # recommendations= filtered_categories.select('asins', 'titles', 'salesranks', 'total_reviews', 'avg_rating')
    # calculate the score using min-max normalization
    filtered_categories = filtered_categories.select(matches.columns)
        # combine the two dataframes into a single dataframe
    filtered_categories = filtered_categories.union(matches)
    
    recommendations = filtered_categories.withColumn("score", \
                ((10000000-col("salesranks")) / 10000000 + col("total_reviews") / 100 + col("avg_rating")) / 3)
    recommendations= recommendations.sort(desc('score')).limit(top_n)
    # recommendations.show(truncate=False)
    # recommendations.printSchema()
    recommendations= recommendations.select("asins", "title")
    recommendations.show(truncate=False)
    return recommendations


