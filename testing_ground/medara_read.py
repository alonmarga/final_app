from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Load Parquet data into a Spark DataFrame
df = spark.read.parquet("/home/alonm/final_app/testing_ground/files/sales_data.parquet")


def show_dataframe(df):
    return df.show(5)


def calc_total(df):
    df_with_total = df.withColumn("total_amount", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))
    return df_with_total.show(5)

def group_df(df):
    df_with_total = df.withColumn("total_amount", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))

    # Group by 'product_name' and calculate aggregated metrics
    aggregated_df = df_with_total.groupBy("product_name").agg(
        F.sum("quantity_sold").alias("total_quantity_sold"),
        F.round(F.sum("total_amount"),2).alias("total_sales_amount")
        )

    # Show the aggregated DataFrame
    return aggregated_df.show()


def calc_ave(df):
    average_unit_price = df.agg(F.mean("unit_price").alias("average_unit_price")).collect()[0]["average_unit_price"]
    return print(f"The average unit price across all transactions is: {average_unit_price:.2f}")

def create_bar_chart(df):
    aggregated_df = df.groupBy("product_name").agg(
        F.sum("quantity_sold").alias("total_quantity_sold")
    )

    # Convert the Spark DataFrame to a Pandas DataFrame
    aggregated_pd_df = aggregated_df.toPandas()

    # Plotting
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 6))
    barplot = sns.barplot(x="product_name", y="total_quantity_sold", data=aggregated_pd_df)
    plt.title('Total Quantity Sold for Each Product')
    plt.xlabel('Product Name')
    plt.ylabel('Total Quantity Sold')
    return plt.show()


def line_chart(df):
    # Calculate the total sales amount for each transaction and round to 2 decimal places
    df_with_total = df.withColumn("total_amount", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))

    # Create a new column with just the year and month
    df_with_month = df_with_total.withColumn("transaction_year_month", F.date_format("transaction_date", "yyyy-MM"))

    # Group by the transaction_year_month and sum the total_amount to get monthly sales
    aggregated_df = df_with_month.groupBy("transaction_year_month").agg(
        F.sum("total_amount").alias("monthly_total_sales")
    ).orderBy("transaction_year_month")

    # Convert the Spark DataFrame to a Pandas DataFrame
    aggregated_pd_df = aggregated_df.toPandas()

    # Plotting
    plt.figure(figsize=(15, 6))
    sns.lineplot(x="transaction_year_month", y="monthly_total_sales", data=aggregated_pd_df)
    plt.title('Monthly Total Sales Amount Over Time')
    plt.xlabel('Transaction Year-Month')
    plt.ylabel('Monthly Total Sales Amount')
    plt.show()
    return print(aggregated_pd_df)

def top_5(df):
        # Calculate the total sales amount for each transaction and round to 2 decimal places
    df_with_total = df.withColumn("total_amount", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))

    # Group by 'product_name' and calculate total sales amount
    aggregated_df = df_with_total.groupBy("product_name").agg(
        F.round(F.sum("total_amount"),2).alias("total_sales_amount")
    )

    # Sort by total_sales_amount in descending order and take the top 5 products
    top_5_products = aggregated_df.sort(F.desc("total_sales_amount")).limit(5)

    # Show the top 5 products
    return top_5_products.show()

def overall_rev(df):
    # Calculate the total sales amount for each transaction and round to 2 decimal places
    df_with_total = df.withColumn("total_amount", F.round(F.col("quantity_sold") * F.col("unit_price"), 2))

    # Calculate overall revenue and average revenue per transaction
    revenue_metrics = df_with_total.agg(
        F.sum("total_amount").alias("overall_revenue"),
        F.mean("total_amount").alias("average_revenue_per_transaction")
    ).collect()[0]

    # Extract the metrics
    overall_revenue = revenue_metrics["overall_revenue"]
    average_revenue_per_transaction = revenue_metrics["average_revenue_per_transaction"]

    # Display the metrics
    print(f"The overall revenue is: {overall_revenue:.2f}")
    print(f"The average revenue per transaction is: {average_revenue_per_transaction:.2f}")

overall_rev(df)