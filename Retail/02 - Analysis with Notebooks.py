# Databricks notebook source
# MAGIC %md
# MAGIC #Analysis with Notebooks
# MAGIC
# MAGIC Databricks Notebooks provide a powerful editor and are perfect for exploratory data analysis. Notebooks operate as a collection of cells which can display markdown (text or images, like this one), code, or output and easy to follow in a linear fashion. Some of the advantages of a Databricks Notebook include:
# MAGIC * Code in multiple languages (Python, SQL, Scala, & R) and mix and match in the same notebook
# MAGIC * Commenting, sharing, and governance for complete collaboration
# MAGIC * Versioning capaiblities and integration with Git
# MAGIC * Variable explorer & MLFlow integration to review what has been acomplished so far
# MAGIC * Integrated with the Databricks Assistant as your companion peer-programmer
# MAGIC * Scheduler or run mutiple notebooks in an orchestrated Workflow
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Whats my main analysis tool in Databricks? Spark!
# MAGIC The power of Databricks is working with big data, or data volumes too large to fit in the memory of a single computer. We can leverage Spark clusters (those things we create in the compute tab) to increase our working pool of memory across many machines and make quick analysis work of even the largest data sets on the planet. 
# MAGIC
# MAGIC The default language of this notebook is python, so lets use some pyspark to read our tables created in the Data Engineering Notebook. We will be working with Spark's concept of Data Frames or an in-memory, tabular representation of our data which we can wrangle and analyze.

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

# DBTITLE 1,Read the users table and display
users_df = spark.read.table('churn_users')
display(users_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC  **Notice the output of the display command are a table of results displayed right in the notebook with full interactivity to scroll, search, and filter.**
# MAGIC
# MAGIC  Click the plus icon next to "Table" to try the other output options:
# MAGIC  * Data Profile - which provides summary statistics and histograms of the features in the data frame
# MAGIC  * Visualization - which provides an interactive visualization builder. Try a bar chart with age_group in the x group and add a y group with the "*"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lets pivot our analysis to answer a real question: 
# MAGIC ####What is the total amount spent by day, by channel in the United States?
# MAGIC We will need our Users table and our Orders table. Lets first refresh ourselves on the Orders table.

# COMMAND ----------

# DBTITLE 1,Display the orders table
spark.read.table('churn_orders').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Our steps will include:**
# MAGIC * Retrieve the relevant columns from orders and users tables
# MAGIC * Join the tables together
# MAGIC * Filter for users in the United States
# MAGIC * Create a clean date field to aggregate on from the transaction timestamp
# MAGIC * Sum the amount x item count for each transaction date in orders table
# MAGIC * Group by the sales channel
# MAGIC * Remove null values

# COMMAND ----------

# DBTITLE 1,Pyspark Analysis Query
import pyspark.sql.functions as F

total_revenue_by_day_channel = (
    spark.read.table('churn_orders') # Read orders
    .select('user_id', 'amount', 'item_count', 'transaction_date')
    .join(spark.read.table('churn_users').select('user_id', 'country', 'channel'), 'user_id') # Join Users
    .filter("country = 'USA'") # Filter for USA
    .withColumn('transaction_date', F.to_timestamp('transaction_date', 'dd-MM-yyyy HH:mm:ss')) 
    .withColumn('transaction_date', F.to_date('transaction_date')) # Keep just the day portion of the timestamp
    .withColumn('total_revenue', F.col('amount') * F.col('item_count')) # Create a total revenue column
    .groupBy('transaction_date', 'channel') # Group by day and channel
    .sum('total_revenue') # Aggregate total revenue
    .filter(F.col('transaction_date').isNotNull() & F.col('channel').isNotNull()) # Drop rows with null value
)

display(total_revenue_by_day_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Woah, woah, woah, but Lukas...
# MAGIC I haven't taken my Apache Spark Developer enablement offered by Databricks through the Customer Academy, and while I can appreciate the query, my pyspark just isnt that advanced.
# MAGIC
# MAGIC ### No problem! We've got options (aside from the training and certifications offered). Lets try that again using SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH orders AS (
# MAGIC     SELECT user_id, 
# MAGIC     amount, 
# MAGIC     item_count, 
# MAGIC     to_date(transaction_date, 'dd-MM-yyyy HH:mm:ss') AS transaction_date
# MAGIC     FROM churn_orders
# MAGIC ),
# MAGIC users AS (
# MAGIC     SELECT user_id, country, channel
# MAGIC     FROM churn_users
# MAGIC )
# MAGIC SELECT orders.transaction_date,
# MAGIC        users.channel,
# MAGIC        SUM(orders.amount * orders.item_count) AS total_revenue
# MAGIC FROM orders
# MAGIC JOIN users ON orders.user_id = users.user_id
# MAGIC WHERE users.country = 'USA'
# MAGIC       AND orders.transaction_date IS NOT NULL
# MAGIC       AND users.channel IS NOT NULL
# MAGIC GROUP BY transaction_date, users.channel

# COMMAND ----------

# MAGIC %md
# MAGIC ### Theres one more option, spark.sql()
# MAGIC spark.sql lets you pass in a SQL statement as text to the pyspark API. This might be helpful when you like using SQL for querying data but need procedural logic like looping or if/else from python.

# COMMAND ----------

total_revenue_by_day_channel2 = spark.sql(
  """
  WITH orders AS (
    SELECT user_id, 
    amount, 
    item_count, 
    to_date(transaction_date, 'dd-MM-yyyy HH:mm:ss') AS transaction_date
    FROM churn_orders
),
users AS (
    SELECT user_id, country, channel
    FROM churn_users
)
SELECT orders.transaction_date,
       users.channel,
       SUM(orders.amount * orders.item_count) AS total_revenue
FROM orders
JOIN users ON orders.user_id = users.user_id
WHERE users.country = 'USA'
      AND orders.transaction_date IS NOT NULL
      AND users.channel IS NOT NULL
GROUP BY transaction_date, users.channel
  """
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What if I get stuck or need help? The Databricks Assistant!
# MAGIC Open the assistant panel on the left and ask a question like: "how many unique users are in churn_users?"
# MAGIC
# MAGIC Then click the double right arrows to replace your current cell with the Assistant's code.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SAMPLE from the Assistant, yours may vary
# MAGIC SELECT COUNT(DISTINCT user_id) AS unique_users
# MAGIC FROM churn_users
