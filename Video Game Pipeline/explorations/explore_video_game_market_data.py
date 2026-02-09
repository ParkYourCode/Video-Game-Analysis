# Databricks notebook source
# MAGIC %md
# MAGIC ### Exploratory Notebook
# MAGIC
# MAGIC Use this notebook to explore interesting video game data insights and trends, influenced by region, publisher, platform, etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate the percentage of total sales contributed by the top 10% of games
# MAGIC WITH ranked_games AS (
# MAGIC   SELECT
# MAGIC     game_key,
# MAGIC     global_sales,
# MAGIC     NTILE(10) OVER (ORDER BY global_sales DESC) AS decile
# MAGIC   FROM workspace.`03_gold`.fact_game_stats
# MAGIC )
# MAGIC SELECT
# MAGIC   (SUM(CASE WHEN decile = 1 THEN global_sales END) /
# MAGIC   SUM(global_sales)) * 100 AS top_10pct_sales_share
# MAGIC FROM ranked_games;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarizes total global sales by genre, ordered from highest to lowest
# MAGIC SELECT
# MAGIC   g.genre_name,
# MAGIC   SUM(s.global_sales) AS total_sales
# MAGIC FROM workspace.`03_gold`.fact_game_stats s
# MAGIC JOIN workspace.`03_gold`.dim_genre g ON s.genre_key = g.genre_key
# MAGIC GROUP BY g.genre_name
# MAGIC ORDER BY total_sales DESC;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarizes average and total games released per platform, ordered from highest to lowest average sales
# MAGIC SELECT
# MAGIC   p.platform_name,
# MAGIC   CEIL(AVG(s.global_sales)) AS avg_sales_per_game,
# MAGIC   COUNT(*) AS title_count
# MAGIC FROM workspace.`03_gold`.fact_game_stats s
# MAGIC JOIN workspace.`03_gold`.dim_platform p ON s.platform_key = p.platform_key
# MAGIC GROUP BY p.platform_name
# MAGIC ORDER BY avg_sales_per_game DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculates total sales and sales share by region
# MAGIC SELECT
# MAGIC   region,
# MAGIC   SUM(sales) AS region_sales,
# MAGIC   (SUM(sales) / SUM(SUM(sales)) OVER ()) * 100 AS region_share
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     "Japan" AS region,
# MAGIC     SUM(jp_sales) AS sales
# MAGIC   FROM workspace.`03_gold`.fact_game_stats s
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     "North America" AS region,
# MAGIC     SUM(na_sales) AS sales
# MAGIC   FROM workspace.`03_gold`.fact_game_stats s
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     "Europe" AS region,
# MAGIC     SUM(eu_sales) AS sales
# MAGIC   FROM workspace.`03_gold`.fact_game_stats s
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     "Other" AS region,
# MAGIC     SUM(other_sales) AS sales
# MAGIC   FROM workspace.`03_gold`.fact_game_stats s
# MAGIC )
# MAGIC GROUP BY region
# MAGIC ORDER BY region_sales DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summarizes average sales per title and total games released per publisher
# MAGIC SELECT
# MAGIC   pub.publisher_name,
# MAGIC   COUNT(*) AS titles_released,
# MAGIC   CEIL(AVG(s.global_sales)) AS avg_sales_per_title
# MAGIC FROM workspace.`03_gold`.fact_game_stats s
# MAGIC JOIN workspace.`03_gold`.dim_publisher pub ON s.publisher_key = pub.publisher_key
# MAGIC GROUP BY pub.publisher_name
# MAGIC HAVING COUNT(*) >= 5
# MAGIC ORDER BY avg_sales_per_title DESC;
