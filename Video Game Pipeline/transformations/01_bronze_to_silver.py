from pyspark.sql.functions import col, to_date, from_unixtime
from pyspark import pipelines as dp

catalog = "workspace"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

# --- Clean Kaggle data ---
@dp.table(
    name=f"{catalog}.{silver_schema}.kaggle_games",
    comment="Cleaned kaggle games",
    table_properties={
        "quality": "silver"
    }
)
@dp.expect_all_or_drop({
    "valid_id": "id > 0 AND id = int(id)",
    "valid_name": "typeof(name) = 'string'",
    "valid_platform": "typeof(platform) = 'string'",
    "valid_year": "year >= 1000 AND year <= 9999 AND year = int(year)",
    "valid_genre": "typeof(genre) = 'string'",
    "valid_publisher": "typeof(publisher) = 'string'",
    "valid_na_sales": "na_sales >= 0",
    "valid_eu_sales": "eu_sales >= 0",
    "valid_jp_sales": "jp_sales >= 0",
    "valid_other_sales": "other_sales >= 0",
    "valid_global_sales": "global_sales >= 0"
})
def kaggle_games():
    df = spark.read.table("workspace.01_bronze.kaggle_games")
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    df = df.withColumnRenamed("rank", "id")
    df = df.filter(col("global_sales") >= 1)
    df = (
        df
        .withColumn("na_sales", (col("na_sales") * 1000000).cast("int"))
        .withColumn("eu_sales", (col("eu_sales") * 1000000).cast("int"))
        .withColumn("jp_sales", (col("jp_sales") * 1000000).cast("int"))
        .withColumn("other_sales", (col("other_sales") * 1000000).cast("int"))
        .withColumn("global_sales", (col("global_sales") * 1000000).cast("int"))
    )

    return df.orderBy(col("id").asc())

# --- Clean IGDB data ---
@dp.table(
    name=f"{catalog}.{silver_schema}.igdb_games",
    comment="Cleaned IGDB games",
    table_properties={
        "quality": "silver"
    }
)
@dp.expect_all_or_fail({
    "valid_id": "id > 0 AND id = int(id)",
    "valid_name": "name IS NOT NULL",
    "valid_rating": "rating >= 0",
    "valid_rating_count": "rating_count >= 0",
    "valid_want_to_play_score": "want_to_play_score >= 0",
    "valid_played_score": "played_score >= 0",
    "valid_total_reviews_score": "total_reviews_score >= 0"
})
def igdb_games():
    df = spark.read.table("workspace.01_bronze.igdb_games")

    return (
        df.select([col(c).alias(c.lower()) for c in df.columns])
        .withColumnRenamed("first_release_date", "release_date")
        .withColumn("release_date", to_date(from_unixtime(col("release_date").cast("bigint"))))
        .withColumnRenamed("total_rating", "rating")
        .withColumnRenamed("total_rating_count", "rating_count")
        .withColumn("rating_count", col("rating_count").cast("int"))
        .withColumn("want_to_play_score", (col("want_to_play_score") * 10000000).cast("double"))
        .withColumn("played_score", (col("played_score") * 10000000).cast("double"))
        .fillna(0, subset=["rating", "rating_count", "want_to_play_score", "played_score", "total_reviews_score"])
        .drop("visits_score", "playing_score", "negative_reviews_score", "global_top_sellers_score", "34_score")
    )