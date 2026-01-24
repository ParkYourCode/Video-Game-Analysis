from pyspark.sql.functions import col, date_format, initcap, monthname, dayofweek, when, concat, lit, expr, year, month, quarter, monotonically_increasing_id
from pyspark import pipelines as dp

catalog = "workspace"
silver_schema = "02_silver"
gold_schema = "03_gold"

# --- Create Date dimension table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.dim_date",
    comment="Date dimension table with day-level granularity",
    table_properties={
        "quality": "gold"
    }
)
def dim_date():
    igdb_df = spark.read.table(f"{catalog}.{silver_schema}.igdb_games")

    min_date = igdb_df.agg({"release_date": "min"}).collect()[0][0]
    max_date = igdb_df.agg({"release_date": "max"}).collect()[0][0]

    date_df = spark.sql(f"""
        SELECT EXPLODE(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) as full_date
    """)

    return (
        date_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("full_date")))
        .withColumn("quarter", concat(lit("Q"), quarter(col("full_date"))))
        .withColumn("month", month(col("full_date")))
        .withColumn("month_name", initcap(monthname(col("full_date"))))
        .withColumn("year_month", date_format(col("full_date"), "yyyy-MM"))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("is_weekend", when(dayofweek(col("full_date")).isin(1, 7), True).otherwise(False))
        .withColumn("decade", concat(expr("floor(year/10) * 10"), lit("s")))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "year_month",
            "day_of_week",
            "is_weekend",
            "decade"
        )
    )

# --- Create Platform dimension table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.dim_platform",
    comment="Platform dimension table",
    table_properties={
        "quality": "gold"
    }
)
def dim_platform():
    kaggle_df = spark.read.table(f"{catalog}.{silver_schema}.kaggle_games")

    platform_df = kaggle_df.select("platform").distinct()

    platform_df = kaggle_df.select("platform").distinct().withColumn(
        "platform_category",
        when(col("platform").isin("Wii", "NES", "SNES", "N64", "GC", "WiiU", "PS3", "PS2", "PS4", "PS", "PSV", "X360", "XB", "XOne", "GEN", "DC", "SAT", "SCD"), "Console")
        .when(col("platform").isin("GB", "DS", "GBA", "3DS", "PSP"), "Handheld")
        .when(col("platform") == "PC", "PC")
        .otherwise("Other")
    ).withColumn(
        "manufacturer",
        when(col("platform").isin("Wii", "NES", "SNES", "N64", "GC", "WiiU", "GB", "DS", "GBA", "3DS"), "Nintendo")
        .when(col("platform").isin("PS3", "PS2", "PS4", "PS", "PSV", "PSP"), "Sony")
        .when(col("platform").isin("X360", "XB", "XOne"), "Microsoft")
        .when(col("platform").isin("GEN", "DC", "SAT", "SCD"), "Sega")
        .otherwise("Other")
    )

    return (
        platform_df
        .withColumn("platform_key", monotonically_increasing_id() + 1)
        .select(
            "platform_key", 
            col("platform").alias("platform_name"),
            "platform_category",
            "manufacturer"
        )
    )

# --- Create Genre dimension table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.dim_genre",
    comment="Genre dimension table",
    table_properties={
        "quality": "gold"
    }
)
def dim_genre():
    kaggle_df = spark.read.table(f"{catalog}.{silver_schema}.kaggle_games")

    genre_df = kaggle_df.select("genre").distinct()

    return (
        genre_df
        .withColumn("genre_key", monotonically_increasing_id() + 1)
        .select(
            "genre_key", 
            col("genre").alias("genre_name")
        )
    )

# --- Create Publisher dimension table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.dim_publisher",
    comment="Publisher dimension table",
    table_properties={
        "quality": "gold"
    }
)
def dim_publisher():
    kaggle_df = spark.read.table(f"{catalog}.{silver_schema}.kaggle_games")

    publisher_df = kaggle_df.select("publisher").distinct()

    return (
        publisher_df
        .withColumn("publisher_key", monotonically_increasing_id() + 1)
        .select(
            "publisher_key", 
            col("publisher").alias("publisher_name")
        )
    )

# --- Create Game dimension table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.dim_game",
    comment="Game dimension table",
    table_properties={
        "quality": "gold"
    }
)
def dim_game():
    kaggle_df = spark.read.table(f"{catalog}.{silver_schema}.kaggle_games")

    game_df = kaggle_df.select("name").distinct()

    return (
        kaggle_df
        .withColumn("game_key", monotonically_increasing_id() + 1)
        .select(
            "game_key",
            col("name").alias("game_name")
        )
    )

# --- Create Fact table ---
@dp.table(
    name=f"{catalog}.{gold_schema}.fact_game_stats",
    comment="Game stats fact table",
    table_properties={
        "quality": "gold"
    }
)
def fact_game_stats():
    kaggle_df = spark.read.table(f"{catalog}.{silver_schema}.kaggle_games")
    igdb_df = spark.read.table(f"{catalog}.{silver_schema}.igdb_games")

    dim_game_df = spark.read.table(f"{catalog}.{gold_schema}.dim_game")
    dim_platform_df = spark.read.table(f"{catalog}.{gold_schema}.dim_platform")
    dim_genre_df = spark.read.table(f"{catalog}.{gold_schema}.dim_genre")
    dim_publisher_df = spark.read.table(f"{catalog}.{gold_schema}.dim_publisher")

    fact_df = (
        kaggle_df
        .join(dim_game_df, kaggle_df.name == dim_game_df.game_name, "inner")
        .join(dim_platform_df, kaggle_df.platform == dim_platform_df.platform_name, "inner")
        .join(dim_genre_df, kaggle_df.genre == dim_genre_df.genre_name, "inner")
        .join(dim_publisher_df, kaggle_df.publisher == dim_publisher_df.publisher_name, "inner")
        .join(igdb_df, kaggle_df.name == igdb_df.name, "inner")
        .withColumn("release_date_key", date_format(col("release_date"), "yyyyMMdd").cast("int"))
    )

    return (
        fact_df
        .select(
            "game_key",
            "platform_key",
            "genre_key",
            "publisher_key",
            "release_date_key",
            "na_sales",
            "eu_sales",
            "jp_sales",
            "other_sales",
            "global_sales",
            "rating",
            "rating_count",
            "want_to_play_score",
            "played_score",
            "total_reviews_score"
        )
    )
