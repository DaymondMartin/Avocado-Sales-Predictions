#!/usr/bin/env python
# coding: utf-8

# ## ML Notebook 
# 
# New notebook

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[ ]:


df = spark.sql("SELECT * FROM Avocado_Gold_Insights.dbo_Kenya_Month_Avocado_Sales LIMIT 1000")
display(df)


# In[ ]:


from pyspark.sql.functions import month, to_date

# Convert the Month column to a date format and extract the month number
df_cleaned = df.withColumn("Month_Num", month(to_date("Month", "MMMM")))

# Drop the 'Month' column
df_cleaned = df_cleaned.drop("Month")

# Use the display function to show the updated DataFrame
display(df_cleaned)

# Verify that the 'Month' column has been removed and the 'Month_Num' column is added
df_cleaned.printSchema()


# In[ ]:


display(df_cleaned)


# In[ ]:


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

# Step 1: Prepare the data for ML
assembler = VectorAssembler(inputCols=["Month_Num", "Average_Price"], outputCol="features")

# Step 2: Assemble the features for training
df_ml = assembler.transform(df_cleaned)

# Step 3: Prepare the data for training
train_data = df_ml.select("features", "Production")

# Show the transformed data
train_data.show(5)


# In[ ]:


# Step 4: Train a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="Production")

# Step 5: Fit the model on the training data
lr_model = lr.fit(train_data)

# Step 6: Get the model coefficients and intercept
print("Coefficients: ", lr_model.coefficients)
print("Intercept: ", lr_model.intercept)

# Step 7: Make predictions on the training data
predictions = lr_model.transform(train_data)

# Show a few predictions
predictions.select("features", "Production", "prediction").show(5)


# In[ ]:


from pyspark.ml.evaluation import RegressionEvaluator

# Step 8: Evaluate the model
evaluator = RegressionEvaluator(labelCol="Production", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on training data = %g" % rmse)

# Step 9: Calculate R2 (R-Squared)
r2_evaluator = RegressionEvaluator(labelCol="Production", predictionCol="prediction", metricName="r2")
r2 = r2_evaluator.evaluate(predictions)
print("R2 on training data = %g" % r2)


# In[ ]:


# Step 1: Train the Linear Regression Model
from pyspark.ml.regression import LinearRegression

# Assuming you have transformed your DataFrame and prepared it for ML
lr = LinearRegression(featuresCol="features", labelCol="Production")

# Fit the model using your training data
lr_model = lr.fit(train_data)

# Now, the trained model is stored in the variable lr_model
# You can use this variable for predictions, evaluation, etc.


# In[ ]:


from pyspark.sql import functions as F

# Generate the data for 2025-2030 (12 months for each year)
# Example: Use a constant value for 'Average_Price' (adjust based on your prediction model)
months_2025_2030 = [(year, month, 10.5) for year in range(2025, 2031) for month in range(1, 13)]
columns = ["Year", "Month_Num", "Average_Price"]

# Create a DataFrame for 2025-2030
df_2025_2030 = spark.createDataFrame(months_2025_2030, columns)

# Step 1: Transform the data for prediction using the same assembler
df_2025_2030_transformed = assembler.transform(df_2025_2030)

# Step 2: Use the trained model to make predictions for 2025-2030
predictions_2025_2030 = lr_model.transform(df_2025_2030_transformed)

# Show the predictions
predictions_2025_2030.select("Year", "Month_Num", "Average_Price", "prediction").show()



# In[ ]:


display(df_2025_2030_transformed)


# In[ ]:


display(predictions_2025_2030)


# In[ ]:


from pyspark.sql.functions import col

# Create a new column for sales prediction (Average_Price * prediction)
sales_prediction_2025 = predictions_2025_2030.filter(predictions_2025_2030.Year == 2025).withColumn(
    "Sales_Prediction", 
    col("Average_Price") * col("prediction")
)

# Show the results
sales_prediction_2025.select("Year", "Month_Num", "Average_Price", "prediction", "Sales_Prediction").show()


# In[ ]:


# Create a new column for sales prediction (Average_Price * prediction)
sales_prediction = predictions_2025_2030.withColumn(
    "Sales_Prediction", 
    col("Average_Price") * col("prediction")
)

# Aggregate the sales prediction by year and sum the values
total_sales_per_year = sales_prediction.groupBy("Year").agg(
    {"Sales_Prediction": "sum"}
)

# Rename the column for better clarity
total_sales_per_year = total_sales_per_year.withColumnRenamed("sum(Sales_Prediction)", "Total_Sales_Prediction")

# Show the results
total_sales_per_year.show()



# In[ ]:


# Drop the 'features' column
predictions_2025_2030 = predictions_2025_2030.drop("features")

# Now, save the DataFrame to the lakehouse with the updated schema
predictions_2025_2030.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Avocado_Gold_Insights.ml_predicted_production_2030")

# Verify by displaying the data
display(predictions_2025_2030)




# In[ ]:


df = spark.sql("SELECT * FROM Avocado_Gold_Insights.ml_predicted_production_2030 LIMIT 1000")
display(df)

