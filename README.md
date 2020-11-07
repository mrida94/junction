# Junction-2020 Project submission Team Outliers
This repository contains team outliers source code.
### Challenges attended
1. Finnish National Agency for Education
2. Paulig, Cafe Intelligence Served

### Links to demos
1.
2. https://datastudio.google.com/reporting/35532be8-34f1-43a3-b4ff-47be8413c40d


# Finnish National Agency for Education, Visualize Educational Development Statistics


The main focus of our solution was to create an elegant, intuitive to use analysis tool that
makes finding deep, but useful and actionable insights from the data easy.

First we created data pipeline to parse unstructured data. Then data model was created to make data usable for analytics and visualization. Finally we created dashboard for to meaningful insights from the data. 

## Solution

Parsing of json dump data was done with python script. Data is parsed into data tables and saved as csv files. Parsed data tables are joined in Tableau. In the tableau dashboard you can search and visualize data. Architecture is as follows: 

![alt text](https://github.com/mrida94/junction/blob/main/opetushallitus/architecture.JPG) 

opetushallitus.py is the main function and free Tableau reader can be used to open the interactive user interface opetushallitus.twbx 

# Paulig, Cafe Intelligence Served

Transaction and weather data from Paulig's customer was utilized in ML model to predict hourly revenue. Model results and feature data are presented in google data studio dashboard. Insight from the dashboard can used to plan shfits, opening hours, and resupply of storage.

## Solution
Gradient boosting regression model was trained based on transaction and wheater data. Results are saved to google BigQuery data warehouse. Dashboard reads data from BigQuery.
