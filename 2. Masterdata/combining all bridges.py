# Generated from: combining all bridges.ipynb
# Converted at: 2026-01-28T04:21:21.375Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

/Volumes/workspace/default/rawdata/steam/application_categories.csv

app_devs_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/application_developers.csv")
)

developers_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/developers.csv")
)


display(app_devs_df.printSchema())
app_devs_df.head(5)


display(developers_df.printSchema())
developers_df.head(5)

from pyspark.sql.functions import collect_set, concat_ws, col

app_developers_agg_df = (
    app_devs_df
    .join(
        developers_df,
        app_devs_df.developer_id == developers_df.id,
        how="left"
    )
    .groupBy(app_devs_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("developers")
    )
)


display(app_developers_agg_df.limit(10))


app_publishers_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/application_publishers.csv")
)

publishers_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/publishers.csv")
)


# Schema check
app_publishers_df.printSchema()
publishers_df.printSchema()

# Sample rows
display(app_publishers_df.limit(5))
display(publishers_df.limit(5))

# Row counts (sanity)
print("application_publishers rows:", app_publishers_df.count())
print("publishers rows:", publishers_df.count())


from pyspark.sql.functions import collect_set, concat_ws, col

app_publishers_agg_df = (
    app_publishers_df
    .join(
        publishers_df,
        app_publishers_df.publisher_id == publishers_df.id,
        how="left"
    )
    .groupBy(app_publishers_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("publishers")
    )
)


# Schema check
app_publishers_agg_df.printSchema()

# Sample rows
display(app_publishers_agg_df.limit(10))

# Row count sanity
print("Aggregated publishers rows:", app_publishers_agg_df.count())

# Null check (should be low / zero)
from pyspark.sql.functions import count, when

app_publishers_agg_df.select(
    count(when(col("publishers").isNull(), 1)).alias("null_publishers")
).show()


app_genres_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/application_genres.csv")
)

genres_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/genres.csv")
)


# Schema check
app_genres_df.printSchema()
genres_df.printSchema()

# Sample rows
display(app_genres_df.limit(5))
display(genres_df.limit(5))

# Row counts
print("application_genres rows:", app_genres_df.count())
print("genres rows:", genres_df.count())


from pyspark.sql.functions import collect_set, concat_ws, col

app_genres_agg_df = (
    app_genres_df
    .join(
        genres_df,
        app_genres_df.genre_id == genres_df.id,
        how="left"
    )
    .groupBy(app_genres_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("genres")
    )
)


# Schema check
app_genres_agg_df.printSchema()

# Sample rows
display(app_genres_agg_df.limit(10))

# Row count sanity
print("Aggregated genres rows:", app_genres_agg_df.count())

# Null check
from pyspark.sql.functions import count, when

app_genres_agg_df.select(
    count(when(col("genres").isNull(), 1)).alias("null_genres")
).show()


app_categories_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/application_categories.csv")
)

categories_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/categories.csv")
)


# Schema check
app_categories_df.printSchema()
categories_df.printSchema()

# Sample rows
display(app_categories_df.limit(5))
display(categories_df.limit(5))

# Row counts
print("application_categories rows:", app_categories_df.count())
print("categories rows:", categories_df.count())


from pyspark.sql.functions import collect_set, concat_ws, col

app_categories_agg_df = (
    app_categories_df
    .join(
        categories_df,
        app_categories_df.category_id == categories_df.id,
        how="left"
    )
    .groupBy(app_categories_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("categories")
    )
)


# Schema check
app_categories_agg_df.printSchema()

# Sample rows
display(app_categories_agg_df.limit(10))

# Row count sanity
print("Aggregated categories rows:", app_categories_agg_df.count())

# Null check
from pyspark.sql.functions import count, when

app_categories_agg_df.select(
    count(when(col("categories").isNull(), 1)).alias("null_categories")
).show()


app_platforms_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/application_platforms.csv")
)

platforms_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/platforms.csv")
)


# Schema check
app_platforms_df.printSchema()
platforms_df.printSchema()

# Sample rows
display(app_platforms_df.limit(5))
display(platforms_df.limit(5))

# Row counts
print("application_platforms rows:", app_platforms_df.count())
print("platforms rows:", platforms_df.count())


from pyspark.sql.functions import collect_set, concat_ws, col

app_platforms_agg_df = (
    app_platforms_df
    .join(
        platforms_df,
        app_platforms_df.platform_id == platforms_df.id,
        how="left"
    )
    .groupBy(app_platforms_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("platforms")
    )
)


# Schema check
app_platforms_agg_df.printSchema()

# Sample rows
display(app_platforms_agg_df.limit(10))

# Row count sanity
print("Aggregated platforms rows:", app_platforms_agg_df.count())

# Null check
from pyspark.sql.functions import count, when

app_platforms_agg_df.select(
    count(when(col("platforms").isNull(), 1)).alias("null_platforms")
).show()


app_developers_agg_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_developers/")


app_publishers_agg_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_publishers/")


app_genres_agg_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_genres/")


app_categories_agg_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_categories/")


app_platforms_agg_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_platforms/")


dbutils.fs.ls("/Volumes/workspace/default/rawdata/steam/processed/aggregates/")