# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 19:12:29 2019

Assignment 3 Programming for Big Data 

This program takes data from the Irish Residential Property Price Register and 
processes the data so that the user is able to inspect the data and manipulate
the data.  


@author: Joanne White 
Student Number: A00268096
@author: jo
"""

import numpy as np
import pandas as pd
from pprint import pprint

FILENAME="FinalProjectData.csv"
#"FinalProjectData.csv"

#This will read a file into a dataframe and remove nulls
def read_file_in(file_to_read):
    df = pd.read_csv(file_to_read,index_col=0).fillna(value='')
    return df

#This will allow the user to view and inspect the data
def inspecting_data(df):
    
    #Part 1.1 - No. of rows and columns
    print()
    print(f"Rows in this data set = {df.shape[0]} Columns in this dataset = {df.shape[1]} \n")
    
    #Part 1.2   - 2 most common eircodes
    print(f"The two most common eircodes are \n \
          {df.groupby('eircode').size().sort_values(ascending=False).head(3)} \n")

    #Part 1.3 - Top 10 dates properties registered on
    print(f"The top 10 dates properties were registered on were \n \
            {df.groupby('date').size().sort_values(ascending = False).head(10)} \n")

    #Part 1.4  - The top 10 counties with total properties registered per county
    print(f"The top 10 counties were properties were registered were \n \
            {df.groupby('county').size().sort_values(ascending = False).head(10)} \n")

    #Part 1.5 -  The bottom 5 counties with the total properties registered 
    #per county
    print(f"The bottom 5 counties were properties were registered were \n \
            {df.groupby('county').size().sort_values(ascending = False).tail(5)} \n")
    
    #Part 1.6 - The percentage of houses registered at not full market price and full
    #market price
    print(f"The percentage of houses that did not make full market price = \
          {df.not_full_market.value_counts(normalize=True)[0]*100:.2f}% \n")
    
    #Part 1.7 - The average price across the whole dataset
    price_avg = df.price.mean()
    print(f"The average price for a house in the Ireland between 2010 and 2017 is €{price_avg:,.2f} \n")

#This will allow user to manipulate house price data for 2i
def average_house_price(df):
    pd.options.display.float_format = '€{:,.2f}'.format
    
    #Part 2.1.a - Output average house prices grouped by county for ALL houses in descending 
    #order    
    print('Data Manipulaation section 2')
    print()
    print(f"The average house prices between 2010 and 2017 by county are \n \
          {df.groupby('county')['price'].mean().sort_values(ascending=False)} \n")
    
    #Part 2.1.b - Output average house prices grouped by county for NEW houses in descending 
    #order 
    df_new_house = df[df.description=='New Dwelling house /Apartment']
    print(f"The average house price between 2010 and 2017 for NEW houses by county are \n \
          {df_new_house.groupby('county')['price'].mean().sort_values(ascending=False)} \n")
  
    #Part 2.1.c - Output average house prices grouped by county for SECONDHAND houses in descending 
    #order 
    df_secondhand_house = df[df.description=='Second-Hand Dwelling house /Apartment']
    print(f"The average house price between 2010 and 2017 for SECOND HAND houses by county are \n \
          {df_secondhand_house.groupby('county')['price'].mean().sort_values(ascending=False)} \n")

#Allows data manipluation of for part 2ii
def total_houses(year,county,df_data):
    #print(year,county)
    #creates new column in dataframe for parsed year
    df_data['year'] = df_data.date.str[-4:]
    
    #Displays the total houses per year per county
    filtered_year = df_data[df_data.year == year]
    filtered_year_county = filtered_year[filtered_year.county==county]
    
    return filtered_year_county.shape[0]
    
# Returns the county for each unique route key    
def find_routing_keys(df_data):
    #clean the data
    eircodes = clean_up_eircodes(df_data)
    #remove duplicate keys
    routing_keys_unique = eircodes.drop_duplicates(subset='route_key')
    
    #pass back the unique key and county in dictionary
    route_key_dict = {}
    for index, row in routing_keys_unique.iterrows():
        route_key_dict[row['route_key']]=row['county']
        
    #Prinitng to screen to show outputs    
    pprint(route_key_dict)
        
    return route_key_dict

#Removes rows without eircodes, splicing first 3 letter/nmubers and adds to new column
def clean_up_eircodes(df_data):
    #remove rows woth  nulls nan's and empty f0r eircode
    eircodes = df_data.replace('',np.nan)
    eircodes = eircodes.dropna(subset=['eircode'])
    
    #Take out the first 3 letter / numbers of eircode and add to new column
    eircodes['route_key'] = eircodes.eircode.str[:3]
        
    return eircodes

#Manipulating the data part iv - returns total registrations by size for a route key
def split_routing_key_by_size(routing_key,df_data):
    #Clean eircodes
    eircodes = clean_up_eircodes(df_data)
    #select all records for particular routing key
    requested_route_key_row = eircodes[eircodes.route_key == routing_key]
    
    return requested_route_key_row.size_description.value_counts() 
          
def main():
     df_data = read_file_in(FILENAME)
     
     #View and inspect the data
     inspecting_data(df_data)
     
     #Manipulate the data: section 2 i
     average_house_price(df_data)
     
     #Manipulating the data: section 2 ii
     #Create a list for year and county and loop through both to pass parameters
     years = ['2015','2016']
    
     counties = ['Westmeath','Limerick','Kerry']
     #Loop through years and counties returning total houses registered
     for year in years:
         for county in counties:
             num_houses = total_houses(year,county,df_data)
             print (f"Total number of houses in {county} in {year}: {num_houses}")
         
     #Manipulating the data : section 2 iii
     #find_routing_keys and associated county
     
     find_routing_keys(df_data)
     
      #Manipluating the data: section 2 iv
     routing_key = 'N37'
     total_regs_by_property_size = split_routing_key_by_size(routing_key,df_data)
     print()
     print('Total registrations grouped by size description for ' + routing_key + ':')
     print(total_regs_by_property_size)

# Main
if __name__== "__main__":
    main()