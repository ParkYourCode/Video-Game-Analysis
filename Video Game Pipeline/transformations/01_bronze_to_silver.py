from pyspark.sql.functions import (col, to_date, date_format, trim, initcap, split, size, when, concat, lit, abs, to_timestamp)
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
    return df.orderBy(col("id").asc())

# --- Clean IGDB data ---
@dp.table(
    name=f"{catalog}.{silver_schema}.igdb_games",
    comment="Cleaned IGDB games",
    table_properties={
        "quality": "silver"
    }
)
@dp.expect_all_or_drop({
    "valid_id": "id > 0 AND id = int(id)",
    "valid_release_date": "release_date IS NOT NULL",
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
        .withColumn("release_date", to_date(col("release_date")))
        .withColumnRenamed("total_rating", "rating")
        .withColumnRenamed("total_rating_count", "rating_count")
        .withColumn("rating_count", col("rating_count").cast("int"))
        .drop("visits_score", "playing_score", "negative_reviews_score", "global_top_sellers_score", "34_score")
    )






