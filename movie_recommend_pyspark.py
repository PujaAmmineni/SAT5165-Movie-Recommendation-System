# -*- coding: utf-8 -*-
"""movie_recommend_pyspark.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1a4sZmEynbEXzdw2s2QMulxmnSh9_ZYdO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import ast

spark = SparkSession.builder.appName("MovieDataset").getOrCreate()

movie_data = spark.read.csv("/content/movies_metadata.csv", header=True, inferSchema=True)

movie_data.show(5)

movie_data.columns

movie_data.dtypes

movie_data.count()

#Converting into numerical
movie_data = movie_data.withColumn("budget", col("budget").cast("double"))
movie_data = movie_data.withColumn("id", col("id").cast("long"))
movie_data = movie_data.withColumn("popularity", col("popularity").cast("double"))
movie_data = movie_data.withColumn("revenue", col("revenue").cast("double"))
movie_data = movie_data.withColumn("runtime", col("runtime").cast("double"))
movie_data = movie_data.withColumn("vote_average", col("vote_average").cast("double"))
movie_data = movie_data.withColumn("vote_count", col("vote_count").cast("double"))

movie_data.show(5)

movie_data.dtypes

movie_data.groupBy("video").count().orderBy("count", ascending=False).show()

movie_data = movie_data.filter((F.col("video") == True) | (F.col("video") == False))

movie_data = movie_data.withColumn("video", F.when(F.col("video") == True, 1).otherwise(0))

movie_data = movie_data.withColumn("adult", F.when(F.col("adult") == True, 1).otherwise(0))
movie_data.show()

#Fill the missing value in the numerical column with mean
rev_med = movie_data.approxQuantile("revenue", [0.5], 0.001)[0]
rt_med = movie_data.approxQuantile("runtime", [0.5], 0.001)[0]
v_avg_med = movie_data.approxQuantile("vote_average", [0.5], 0.001)[0]
v_count_med = movie_data.approxQuantile("vote_count", [0.5], 0.001)[0]

movie_impute = movie_data.fillna({
    'revenue': rev_med,
    'runtime': rt_med,
    'vote_average': v_avg_med,
    'vote_count': v_count_med
})

movie_impute.show(5)

movie_impute.count()

#checking null values in every variables
nullvalues_count = movie_impute.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in movie_impute.columns])

nullvalues_count.show()

movie_clean = movie_impute.drop('belongs_to_collection', 'homepage', 'tagline')

movie_clean.show()

movie_clean.count()

#Droping the null values
movie_clean = movie_clean.dropna()

movie_clean.count()

quantiles = movie_clean.approxQuantile("budget", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

print(f"min: {quantiles[0]}")
print(f"Q1: {quantiles[1]}")
print(f"Q2: {quantiles[2]}")
print(f"Q3: {quantiles[3]}")
print(f"max: {quantiles[4]}")

quantiles = movie_clean.approxQuantile("revenue", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# Print the results
print(f"min: {quantiles[0]}")
print(f"Q1: {quantiles[1]}")
print(f"Q2: {quantiles[2]}")
print(f"Q3: {quantiles[3]}")
print(f"max: {quantiles[4]}")

def parse_names(dict_inp):
    if isinstance(dict_inp, str):
        try:

            dict_var = ast.literal_eval(dict_inp)
            return [d['name'] for d in dict_var] if isinstance(dict_var, list) else []
        except:
            return []
    return []

# Register the UDF with PySpark
parsed = udf(parse_names, ArrayType(StringType()))

# Apply the UDF to 'spoken_languages', 'production_countries', 'production_companies', and 'genres'
movie_clean = movie_clean.withColumn("language", parsed(col("spoken_languages")))
movie_clean = movie_clean.withColumn("production_countries", parsed(col("production_countries")))
movie_clean = movie_clean.withColumn("production_companies", parsed(col("production_companies")))
movie_clean = movie_clean.withColumn("genre_names", parsed(col("genres")))

# Show the result with the new extracted columns
movie_clean.select("spoken_languages", "language", "production_countries", "production_companies", "genres", "genre_names").show(5, truncate=False)

movie_clean.columns

movie_clean.count(

)

movie_clean = movie_clean.drop('genres', 'spoken_languages')

movie_clean.show()

spark = SparkSession.builder.appName("DummyVariables").getOrCreate()


#converting categorical col to numeric
indx = StringIndexer(inputCol="status", outputCol="stat_indx")
movie_clean = indx.fit(movie_clean).transform(movie_clean)

# creating dummies
numeric_resp = OneHotEncoder(inputCol="stat_indx", outputCol="stat_dummies")
movie_clean = numeric_resp.fit(movie_clean).transform(movie_clean)

# displaying
movie_clean.select("status", "stat_indx", "stat_dummies").show(5)

movie_clean.show()

cols = ['runtime', 'vote_average']

for col in cols:

    Q_1 = movie_clean.approxQuantile(col, [0.25], 0.0)[0]
    Q_3 = movie_clean.approxQuantile(col, [0.75], 0.0)[0]
    IQR = Q_3 - Q_1

    lr_bound = Q_1 - 1.5 * IQR
    up_bound = Q_3 + 1.5 * IQR

    med_val = movie_clean.approxQuantile(col, [0.5], 0.0)[0]

    print(f"Column: {col}, Lower Bound: {lr_bound}, Upper Bound: {up_bound}, Median value: {med_val}")

    # Replace outliers with median
    movie_clean = movie_clean.withColumn(
        col,
        F.when((F.col(col) < lr_bound) | (F.col(col) > up_bound), med_val)
         .otherwise(F.col(col))
    )

# Show the result after replacing outliers
movie_clean.select(cols).show()

# Cap the values in the 'popularity' column at 5
movie_clean = movie_clean.withColumn("popularity", F.when(F.col("popularity") > 5, 5).otherwise(F.col("popularity")))

#['budget', 'popularity', 'revenue','vote_count'],
quantiles = movie_clean.approxQuantile("popularity", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05)

# Print the results
print(f"min: {quantiles[0]}")
print(f"Q1: {quantiles[1]}")
print(f"Q2: {quantiles[2]}")
print(f"Q3: {quantiles[3]}")
print(f"max: {quantiles[4]}")

movie_clean = movie_clean.drop("vote_count", "poster_path", "status_index", "status", "imdb_id", "original_title", )

# Show the updated DataFrame
movie_clean.show()

movie_clean.count()



summary_stats = movie_clean.select("popularity", "runtime", "vote_average").describe()

summary_stats.show()

cols = ['popularity', 'runtime', 'vote_average']

# Create an empty dictionary to store correlation values
correlations = {}

# Loop through each pair of columns to calculate the correlation
for i in cols:
    for j in cols:
        corr_val = movie_data.stat.corr(i, j)
        correlations[(i, j)] = corr_val

print(correlations)

