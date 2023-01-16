# -*- coding: utf-8 -*-
"""
Created on Fri Nov 4 19:25:03 2022

@author: Meraz Mamun
"""

#Import pandas library to extract data from fortune 500 websites and use pandas related functions for data manipulation
import pandas as pd

#Use to retrieve data from JSON files
import json

#Use these libraries to conncet to MySQL server to transfer the data from python to a SQL database
import sqlalchemy
import pymysql

#Use in conjunction with time library to produce random seconds
import random

#Access time sleep function to slow web scraping process down
import time
    
def fortune_500_archive(begin_year, end_year):
    
    #html_links_list contains the sub urls for fortune 500 websites
    html_links_list = ["1.html", "101.html", "201.html", "301.html", "401.html"]
    
    #The year that you want to start the data collection from
    fortune_year = begin_year
    
    #Access the first sub url in the website
    html_links_list_index = 0
    
    #Represents the next page or year of the website
    next_page = True
    
    #List that contains each year for each row of the dataset
    fortune_year_list = []
    
    #List that contains the ranking of each company throughout the years. Note that the next year rows overlap with the previous year rows
    rank_list = []
    
    #List that separates the overlap. Comes of as a nested list
    rank_all_list = []
    
    #List that contains the name of each company throughout the years. Note that the next year rows overlap with the previous year rows
    company_name_list = []
    
    #List that separates the overlap. Comes of as a nested list
    company_name_all_list = []
    
    #List that removes the nested list by flattening it
    company_name_all_flat_list = []
    
    #List that contains the revenue of each company throughout the years. Note that the next year rows overlap with the previous year rows
    revenue_list = []
    
    #List that separates the overlap. Comes of as a nested list
    revenue_all_list = []
    
    #List that removes the nested list by flattening it
    revenue_all_flat_list = []
    
    #List that contains the profit of each company throughout the years. Note that the next year rows overlap with the previous year rows
    profits_list = []
    
    #List that separates the overlap. Comes of as a nested list
    profits_all_list = []
    
    #List that removes the nested list by flattening it
    profits_all_flat_list = []
    
    #Stay on the current webpage until the code finishes looping through each sub webpage
    while next_page: 
        
        #After looping through each sub webpage, code will move onto next year or next webpage
        while html_links_list_index < 5:
            
            #Access the actual website to retrieve the data
            fortune_500_data = (pd.read_html("https://money.cnn.com/magazines/fortune/fortune500_archive/full/" +
                                str(fortune_year) + "/" + html_links_list[html_links_list_index])[4])
            
            #Slow web scraping process down between 3 seconds and 7 seconds at a time
            time.sleep(random.randint(3, 7))
            
            #Get data from beginning year to end year for each column and append them to the appropriate list
            rank_list = fortune_500_data.Rank.to_list()
            rank_all_list.append(rank_list)
            company_name_list = fortune_500_data.Company.to_list()
            company_name_all_list.append(company_name_list)
            revenue_list = fortune_500_data["Revenues($ millions)"].to_list()
            revenue_all_list.append(revenue_list)
            profits_list = fortune_500_data["Profits($ millions)"].to_list()
            profits_all_list.append(profits_list)
            
            #Move onto next sub webpage
            html_links_list_index = html_links_list_index + 1
        
        #Once last sub webpage is reached, reset the variable to 0 to start from the beginning sub webpage for next year data
        html_links_list_index = 0
        
        #Move onto next year        
        fortune_year = fortune_year + 1
        
        #Check if ending year is reeached
        #If reached, then don't move onto the next page and stop the loop
        if(fortune_year > end_year):
            next_page = False
    
    #Remove each nested list by flattening them
    company_name_all_flat_list = sum(company_name_all_list, [])
    revenue_all_flat_list = sum(revenue_all_list, [])
    profits_all_flat_list = sum(profits_all_list, [])
    
    #Convert each index in profits list into string datatype to keep each datatype for future years consistent
    for k in range(0, len(profits_all_flat_list)):
        profits_all_flat_list[k] = str(profits_all_flat_list[k])
        
    #fortune year will become beginning year again to be used in next loop
    fortune_year = begin_year
    
    #Used to convert ranking data into the data's respective year
    #If ranking is 500, move onto next year to include in the list
    for i in rank_all_list:
        for j in i:
            if(j == 500):
                fortune_year = fortune_year + 1
            j = fortune_year
            fortune_year_list.append(j)
    
    #Reset fortune_year to beginning year again
    fortune_year = begin_year 
    
    #For some reason, index 499 got moved to the last spot on the list and was added as one year after ending year
    #For example, let's say we are looking at data from 1955 to 1957, index 499 is suppose to be the beginning year (e.g. 1955), but was moved to last spot on list and became ending year + 1 (1957 + 1 = 1958)
    #Below two lines of code fixes this issue for the years
    fortune_year_list.insert(499, fortune_year_list.pop())
    fortune_year_list[499] = fortune_year
    
    #return 4 different lists: year, company name, revenue, profit
    return (fortune_year_list, company_name_all_flat_list, revenue_all_flat_list, profits_all_flat_list)

def fortune_500_2006_2012(begin_year, end_year, table_index):
    
    #html_links_list contains the sub urls for fortune 500 websites
    html_links_list = ["index.html", "101_200.html", "201_300.html", "301_400.html", "401_500.html"]
    
    #The year that you want to start the data collection from
    fortune_year = begin_year
    
    #Access the first sub url in the website
    html_links_list_index = 0
    
    #Represents the next page or year of the website
    next_page = True
    
    #List that contains each year for each row of the dataset
    fortune_year_list = []
    
    #List that contains the ranking of each company throughout the years. Note that the next year rows overlap with the previous year rows
    rank_list = []
    
    #List that separates the overlap. Comes of as a nested list
    rank_all_list = []
    
    #List that contains the name of each company throughout the years. Note that the next year rows overlap with the previous year rows
    company_name_list = []
    
    #List that separates the overlap. Comes of as a nested list
    company_name_all_list = []
    
    #List that removes the nested list by flattening it
    company_name_all_flat_list = []
    
    #List that contains the revenue of each company throughout the years. Note that the next year rows overlap with the previous year rows
    revenue_list = []
    
    #List that separates the overlap. Comes of as a nested list
    revenue_all_list = []
    
    #List that removes the nested list by flattening it
    revenue_all_flat_list = []
    
    #List that contains the profit of each company throughout the years. Note that the next year rows overlap with the previous year rows
    profits_list = []
    
    #List that separates the overlap. Comes of as a nested list
    profits_all_list = []
    
    #List that removes the nested list by flattening it
    profits_all_flat_list = []
    
    #Stay on the current webpage until the code finishes looping through each sub webpage
    while next_page: 
        
        #After looping through each sub webpage, code will move onto next year or next webpage
        while html_links_list_index < 5:
            
            #https://money.cnn.com/magazines/fortune/fortune500/2007/full_list/index.html
            
            #Access the actual website to retrieve the data
            fortune_500_data = (pd.read_html("https://money.cnn.com/magazines/fortune/fortune500/" + str(fortune_year) + "/full_list/" +
                                html_links_list[html_links_list_index])[table_index])
            
            #Slow web scraping process down between 3 seconds and 7 seconds at a time
            time.sleep(random.randint(3, 7))
            
            #Get data from beginning year to end year for each column and append them to the appropriate list
         
            rank_list = fortune_500_data.iloc[:, 0].to_list()
            rank_all_list.append(rank_list)
            company_name_list = fortune_500_data.iloc[:, 1].to_list()
            company_name_all_list.append(company_name_list)
            revenue_list = fortune_500_data.iloc[:, 2].to_list()
            revenue_all_list.append(revenue_list)
            profits_list = fortune_500_data.iloc[:, 3].to_list()
            profits_all_list.append(profits_list)
            #print(fortune_500_data.columns)
            #Move onto next sub webpage
            html_links_list_index = html_links_list_index + 1
        
        #Once last sub webpage is reached, reset the variable to 0 to start from the beginning sub webpage for next year data
        html_links_list_index = 0
        
        #Move onto next year        
        fortune_year = fortune_year + 1
        
        #Check if ending year is reeached
        #If reached, then don't move onto the next page and stop the loop
        if(fortune_year > end_year):
            next_page = False
    
    #Remove each nested list by flattening them
    company_name_all_flat_list = sum(company_name_all_list, [])
    revenue_all_flat_list = sum(revenue_all_list, [])
    profits_all_flat_list = sum(profits_all_list, [])
    
    #Convert each index in profits list into string datatype to keep each datatype for future years consistent
    for k in range(0, len(profits_all_flat_list)):
        profits_all_flat_list[k] = str(profits_all_flat_list[k])
        
    #fortune year will become beginning year again to be used in next loop
    fortune_year = begin_year
    
    #Used to convert ranking data into the data's respective year
    #If ranking is 500, move onto next year to include in the list
    for i in rank_all_list:
        for j in i:
            if(j == 500):
                fortune_year = fortune_year + 1
            j = fortune_year
            fortune_year_list.append(j)
    
    #Reset fortune_year to beginning year again
    fortune_year = begin_year 
    
    #For some reason, index 499 got moved to the last spot on the list and was added as one year after ending year
    #For example, let's say we are looking at data from 1955 to 1957, index 499 is suppose to be the beginning year (e.g. 1955), but was moved to last spot on list and became ending year + 1 (1957 + 1 = 1958)
    #Below two lines of code fixes this issue for the years
    fortune_year_list.insert(499, fortune_year_list.pop())
    fortune_year_list[499] = fortune_year
    
    #return 4 different lists: year, company name, revenue, profit
    return (fortune_year_list, company_name_all_flat_list, revenue_all_flat_list, profits_all_flat_list)


def fortune_500_modern(fortune_year):
    
    b = 0
    a = 0
    total_list = []
    fortune_year_list = []
    rank_list = []
    company_list = []
    revenue_list = []
    profit_list = []
    
    #Load the JSON files saved in the "Fortune 500 Data Project" folder based on the appropriate year
    with open("C:/Users/mmamu/OneDrive/Desktop/Fortune 500 Data Project/Fortune Data 2013 to 2022 JSON/fortune_1000_" + str(fortune_year) + ".json") as data:
        fortune_data = json.load(data)
    
    #Delete first item in the list
    del fortune_data[0]
    
    #Index inside first bracket stays constant
    #Index inside second bracket is used to move down by one row
    #Index in third bracket is used to move one column to the right
    #Index in the second bracket should stay constant until index in the third bracket reaches last column    
    while (a < len(fortune_data[0]["items"])):
       while (b < len(fortune_data[0]["items"][0]["fields"])):
           total_list = total_list + list(fortune_data[0]["items"][a]["fields"][b].values())
           b = b + 1
       b = 0
       a = a + 1
    
    #Convert each entry for year, rank, company name, revenue, and profit into its own respective list
    for c in range(0, len(total_list)):
        if(total_list[c] == "rank"):
            rank_list.append(int(total_list[c+1]))
            fortune_year_list.append(fortune_year)
        if(total_list[c] == "revenues" or total_list[c] == "f500_revenues"):
            revenue_list.append(float(total_list[c+1]))
        if(total_list[c] == 'title'):
            company_list.append(total_list[c+1])
        if(total_list[c] == "profits" or total_list[c] == "f500_profits"):
            profit_list.append((total_list[c+1]))

    return (fortune_year_list, company_list, revenue_list, profit_list)


#Convert each list for each year into separate dataframe
#Transpose and rename each dataframe
fortune_data_1955_to_1975 = pd.DataFrame(data = fortune_500_archive(1955, 1975)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_1976_to_1995 = pd.DataFrame(data = fortune_500_archive(1976, 1995)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_1996_to_2005 = pd.DataFrame(data = fortune_500_archive(1996, 2005)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2006_2007 = pd.DataFrame(data = fortune_500_2006_2012(2006, 2007, 5)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2008_2008 = pd.DataFrame(data = fortune_500_2006_2012(2008, 2008, 4)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2009_2012 = pd.DataFrame(data = fortune_500_2006_2012(2009, 2012, 1)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2013 = pd.DataFrame(data = fortune_500_modern(2013)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2014 = pd.DataFrame(data = fortune_500_modern(2014)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2015 = pd.DataFrame(data = fortune_500_modern(2015)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2016 = pd.DataFrame(data = fortune_500_modern(2016)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2017 = pd.DataFrame(data = fortune_500_modern(2017)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2018 = pd.DataFrame(data = fortune_500_modern(2018)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2019 = pd.DataFrame(data = fortune_500_modern(2019)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2020 = pd.DataFrame(data = fortune_500_modern(2020)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2021 = pd.DataFrame(data = fortune_500_modern(2021)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})
fortune_data_500_2022 = pd.DataFrame(data = fortune_500_modern(2022)).transpose().rename(columns = {0 : "Year", 1 : "Company", 2 : "Revenue", 3 : "Profit"})

#Combine each dataframe into a single dataframe, resetting row indexes
fortune_500_final = pd.concat([fortune_data_1955_to_1975, fortune_data_1976_to_1995, 
                                  fortune_data_1996_to_2005, fortune_data_500_2006_2007,
                                  fortune_data_500_2008_2008, fortune_data_500_2009_2012, 
                                  fortune_data_500_2013, fortune_data_500_2014,
                                  fortune_data_500_2015, fortune_data_500_2016, 
                                  fortune_data_500_2017, fortune_data_500_2018,
                                  fortune_data_500_2019, fortune_data_500_2020,
                                  fortune_data_500_2021, fortune_data_500_2022], ignore_index = True)


#Execute this code everytime you use this approach to connect with MySQL
pymysql.install_as_MySQLdb()   
engine = sqlalchemy.create_engine('mysql://root:HXTKGVhxtkgv123.@localhost:3306/')

#Export the fortune_500_final dataframe (which contains all the fortune companies from 1955 to 2022) to MySQL database
fortune_500_final.to_sql(con = engine, schema = "fortune_500", name = "fortune", if_exists = "append", index = False)