# **Avocado Sales and Production Prediction: A Microsoft Fabric and Azure AI Project**

## **Project Overview**

The goal of this project is to predict avocado sales and production trends in Kenya from 2025 to 2030 using Microsoft Fabric and Azure OpenAI. The project integrates historical data, weather conditions, and global market projections to provide actionable insights that help optimize avocado production, stabilize the market, and uncover growth opportunities for farmers and stakeholders in Kenya's avocado industry.

![image](https://github.com/user-attachments/assets/dbe93386-3236-4a7c-abd3-9385ffa33f0c)


### **Key Objectives**
- How can we forecast avocado production and market prices for the next decade?
- How can we optimize production and market strategies for farmers and distributors?
- How can we leverage Microsoft Fabric's unified data platform to manage and analyze large datasets?
- How can we implement machine learning models to predict future avocado sales and production?

## **Technologies Used**

### **1. Microsoft Fabric**

Microsoft Fabric is a cloud-based platform that integrates various data management and analytics tools into a single environment. It helps simplify the entire process of managing data, performing data transformations, and running machine learning models. Key features used in this project include:
- **Data Ingestion**: Using pipelines, dataflows, and event streams to load and process data.
- **Medallion Architecture**: Organizing data into raw, cleaned, and enriched layers for better management and analysis.
- **Azure OpenAI Integration**: For generating business insights and predictions.
![image](https://github.com/user-attachments/assets/65a22815-b54d-4c4f-ae5d-5d13ced61342)

### **2. Azure OpenAI**

Azure OpenAI provides access to powerful language models like GPT for generating text-based insights. We use this to summarize production forecasts and market trends based on our predictions. The integration allows us to generate meaningful insights for stakeholders in the avocado industry.
![image](https://github.com/user-attachments/assets/bd2c2ebd-2d94-4d02-beb6-fa825a64bbb2)

### **3. Machine Learning (ML)**

We used machine learning algorithms to predict avocado production and sales. The model is trained on historical data from the Kenyan avocado industry, including variables like production volume, price, and weather conditions. This allows for forecasting sales and production from 2025 to 2030.
![image](https://github.com/user-attachments/assets/79ba9db8-abca-4735-b52a-7c15d027d552)


## **Data Sources**

The data used in this project comes from the following sources:
- **Kenya's Avocado Sales**: Monthly sales data, including production volume, price, and consumption patterns.
- **Weather Conditions**: Historical weather data influencing avocado growth.
- **Global Market Projections**: Information on global avocado consumption trends and forecasts.

The data was ingested into Microsoft Fabric using the following methods:
- **Pipelines**: To load data from SQL Server.
  ![image](https://github.com/user-attachments/assets/390ce04a-1b90-44c7-ab4f-0b9d7e47c6b7)

- **Dataflows**: To load a local csv file.
  ![image](https://github.com/user-attachments/assets/b311280e-c46b-4a45-a7c7-daafb7a308ed)

- **Notebooks**: To load data from an external API - Google drive.
  ![image](https://github.com/user-attachments/assets/8d1a906d-3ae2-4afa-9ee6-c668bd7321fe)

- **Event Streams**: For real-time weather data from open meteor.
![image](https://github.com/user-attachments/assets/60275bfb-da21-4a6e-b237-69f3254b88f6)


## **Data Transformation and Cleaning**

Before applying machine learning models, the raw data was transformed and cleaned using the following steps:
1. **Handling Missing Data**: Missing values were imputed using appropriate techniques (mean imputation, forward/backward fill).
2. **Date Handling**: The 'Month' column was converted into a numerical representation (`Month_Num`).
3. **Feature Engineering**: Additional features like 'Year' and 'Average_Price' were created to better represent the trends.
![image](https://github.com/user-attachments/assets/3f555a32-8aab-49e4-89da-786bbe6f4914)
![image](https://github.com/user-attachments/assets/95674925-a8c7-473f-9663-2c6ceac3d9da)

We also used **Medallion Architecture** to organize the data into the following layers:
- **Raw Layer**: Data in its original format, directly ingested.
  ![image](https://github.com/user-attachments/assets/52773a2c-e5d8-421e-9c63-d071906f7c32)

- **Clean Layer**: Pre-processed data with missing values handled and transformations applied.
  ![image](https://github.com/user-attachments/assets/14cb5246-117e-4d7e-ba3b-7678efd76d96)

- **Enriched Layer**: Final data with additional features and insights ready for analysis.
![image](https://github.com/user-attachments/assets/069879fb-300c-4725-90ab-05de983e4042)


## **Machine Learning Model**

We built a **Linear Regression model** to predict avocado production and sales based on historical data. The steps involved are:

### **1. Data Preparation**
- **Feature Engineering**: Columns like 'Average_Price' and 'Month_Num' were used as features for prediction.
- **Normalization**: Data was scaled to ensure that all features contribute equally to the model.
![image](https://github.com/user-attachments/assets/b561e549-2259-4208-aeb7-b55841ae342e)

### **2. Model Training**
- **Model Choice**: A linear regression model was used due to its simplicity and ability to handle numerical data.
- **Training**: The model was trained using historical data from 2015 to 2024.
![image](https://github.com/user-attachments/assets/70806e73-2485-4305-b37c-412965d55be5)

### **3. Predictions**
- **Forecasting**: The model was used to predict avocado production and sales from 2025 to 2030.
- **Evaluation**: We evaluated the model using metrics like Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE).
![image](https://github.com/user-attachments/assets/335a4470-7acf-47f3-851a-c24f6d973bdd)


## **Business Insights with Azure OpenAI**

After obtaining the predictions from the model, **Azure OpenAI** was used to generate market insights. The language model takes the predicted data (like production for 2030) and generates a concise business insight for stakeholders. For example:

- **Input to Azure OpenAI**: "Kenya is projected to produce 500,000 kg of avocados in 2030. Provide a short market insight."
- **Output from Azure OpenAI**: "Kenya's avocado production is projected to increase significantly by 2030, presenting opportunities for export growth and expanding into new regional markets."
![image](https://github.com/user-attachments/assets/6aade9da-c837-4290-a47e-d5292b2b1be0)


## **Results and Visualization**

### **1. Data Visualizations**
- **Production Trends**: Graphs showing the predicted production for 2025-2030.
- **Price Trends**: Graphs illustrating the projected price fluctuations in the avocado market.

### **2. Summary Insights**
- **Sales Predictions**: Total predicted sales for the next five years.
- **Production Forecast**: Total predicted production for 2025-2030.

### **3. Market Insights**: Generated using Azure OpenAI, summarizing the impact of predicted changes on the avocado market.
![image](https://github.com/user-attachments/assets/235f22cc-5b43-4ccf-b1c2-2c284bcf274c)



## **GitHub Folder Structure**

The project is organized in the following way on GitHub:

Avocado_AI_Project/
│
├── README.md                  # Project documentation
├── notebooks/                  # Jupyter Notebooks for data exploration and model building
│   └── avocado_sales_analysis.ipynb
├── scripts/                    # Python scripts for preprocessing and model management
│   └── data_preprocessing.py
│   └── model_training.py
├── data/                       # Data storage for avocado sales, production, and predictions
│   └── kenya_avocado_sales.csv
├── models/                     # Saved models and their weights
│   └── avocado_production_model.pkl
└── results/                    # Predictions and insights output
    └── ml_predictions_2030.csv

## **Running the Project**

1. **Clone the repository**:
git clone https://github.com/your-username/Avocado_AI_Project.git


2. **Install dependencies**:
pip install -r requirements.txt


3. **Run the notebooks** for data exploration and model building. Use Jupyter or your preferred notebook environment.

4. **Data Preprocessing**: The preprocessing script will clean and prepare the data.
python scripts/data_preprocessing.py


5. **Model Training**: Train the model with historical data.
python scripts/model_training.py


## **Future Enhancements**

- **Model Optimization**: Experiment with other models like Random Forest or XGBoost for better accuracy.

  MIT Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

