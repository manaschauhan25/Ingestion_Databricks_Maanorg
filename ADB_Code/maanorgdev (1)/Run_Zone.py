# Databricks notebook source
dbutils.widgets.text("Zone_Name","")
ZoneName=dbutils.widgets.getArgument("Zone_Name")

# COMMAND ----------

Notebook_Json_Path = {
    "Raw":["/Users/500067883@stu.upes.ac.in/maanorgdev/RAW_Sourcing/Plane_File_Sourcing"],

    "Cleansed":["/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/Airlines",
                "/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/Airport",
                "/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/Cancellation",
                "/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/Flight",
                "/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/Plane",
                "/Users/500067883@stu.upes.ac.in/maanorgdev/Cleaning_data/UNIQUE_CARRIERS"],
    
    "DataQuality":["/Users/500067883@stu.upes.ac.in/maanorgdev/DataQuality/Data_Quality"],
    
    "Mart":["/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_Airlines",
            "/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_Airport",
            "/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_Cancellation",
            "/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_Flight",
            "/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_Plane",
            "/Users/500067883@stu.upes.ac.in/maanorgdev/MART/DIM_UNIQUE_CARRIERS"]
    
}

# COMMAND ----------

# Notebook_Json_Path[ZoneName]
for i in Notebook_Json_Path[ZoneName]:
    dbutils.notebook.run(i,0)

# COMMAND ----------

["Raw","Cleansed","DataQuality","Mart"]

# COMMAND ----------


