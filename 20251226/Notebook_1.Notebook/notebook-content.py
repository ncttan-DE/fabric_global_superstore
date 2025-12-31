# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0e584ec2-ac1b-4527-a56a-135ad6afbedf",
# META       "default_lakehouse_name": "lh_global_superstore",
# META       "default_lakehouse_workspace_id": "14bc441d-af57-4449-94e9-848ed4994395",
# META       "known_lakehouses": [
# META         {
# META           "id": "0e584ec2-ac1b-4527-a56a-135ad6afbedf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
print("Hi")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs("Files/new_folder")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.ls("Files/raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
