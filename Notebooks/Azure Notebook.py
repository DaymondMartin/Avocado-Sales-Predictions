#!/usr/bin/env python
# coding: utf-8

# ## Azure Notebook
# 
# New notebook

# In[2]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# Importing Libraries

# In[4]:


import requests
import json

# 🔐 Fill in your Azure OpenAI credentials
api_key = "<api key>"  # Replace with your key
endpoint = "<end point>"  # No trailing slash!
deployment_name = "<name>"  # Or gpt-35-turbo

# Full URL with API version
url = f"{endpoint}openai/deployments/{deployment_name}/chat/completions?api-version=2023-07-01-preview"


# In[5]:


headers = {
    "Content-Type": "application/json",
    "api-key": api_key
}

data = {
    "messages": [
        {"role": "system", "content": "You are a helpful data assistant."},
        {"role": "user", "content": "Summarize Kenya's avocado forecast from 2026 to 2030 in two sentences."}
    ],
    "temperature": 0.7,
    "max_tokens": 200
}

response = requests.post(url, headers=headers, data=json.dumps(data))


# In[6]:


if response.status_code == 200:
    result = response.json()
    print("✅ Response:")
    print(result["choices"][0]["message"]["content"])
else:
    print("❌ Error:")
    print("Status Code:", response.status_code)
    print(response.text)


# In[1]:


df = spark.sql("SELECT * FROM ML_Predicted_Production_2030").toPandas()
df.head()


# In[4]:


import requests
import json

# Your Azure OpenAI credentials
api_key = "<api key>"  # Replace with your key
endpoint = "<end point>"  # No trailing slash!
deployment_name = "<name>"  # Or gpt-35-turbo

url = f"{endpoint}openai/deployments/{deployment_name}/chat/completions?api-version=2023-07-01-preview"



# In[5]:


import requests
import json

# Assuming you have already defined the necessary variables:
# api_key = "<your_api_key>"
# url = "<your_endpoint_url>"

def summarize_row(row):
    # Adjusting for the correct column names in your dataset
    prompt = f"Kenya is projected to produce {row['prediction']} kg of avocados in {row['Year']}. Give a short market insight."

    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }

    data = {
        "messages": [
            {"role": "system", "content": "You are an assistant that writes short business insights."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 100
    }

    # Make the request to the OpenAI API
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    else:
        return f"Error {response.status_code}: {response.text}"

# Apply the function to your dataframe
df["Insight"] = df.apply(summarize_row, axis=1)
df.head()  # Or display df to see the new column with insights



# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import requests
import json

# Ensure your Spark session is active and the correct table is loaded
# Load the data into a Spark DataFrame
df = spark.sql("SELECT * FROM ML_Predicted_Production_2030")

# Check the schema to confirm columns are available
df.printSchema()

# Define the function to generate the insight
def summarize_row(year, prediction):
    prompt = f"Kenya is projected to produce {prediction} kg of avocados in {year}. Give a short market insight."

    headers = {
        "Content-Type": "application/json",
        "api-key": api_key  # Make sure to define your API key
    }

    data = {
        "messages": [
            {"role": "system", "content": "You are an assistant that writes short business insights."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 100
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    else:
        return f"Error {response.status_code}: {response.text}"

# Create a UDF (User Defined Function) for Spark
summarize_udf = udf(summarize_row, StringType())

# Apply the UDF to your DataFrame, creating the "Insight" column
df_with_insights = df.withColumn("Insight", summarize_udf("Year", "prediction"))

# Show the results with the new "Insight" column
df_with_insights.show()



# In[6]:


# Save the DataFrame with insights to the lakehouse, overwriting the existing table
df_with_insights.write.format("delta").mode("overwrite").saveAsTable("Avocado_Gold_Insights.ml_predicted_production_insights")

# Optionally, verify by displaying the data
df_with_insights.show(truncate=False)


