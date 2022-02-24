# Spark-Python-POO-y-SparkSQL
Automatic Loader that creates (if necessary) and load with parametry partitions of tables (it infers all the schema by itself).

Its purpose is to generate a backup of all the productive tables that are requested, with an X number of partitions, thus allowing the recovery of lost information in the event of an error.

IMPORTANT: The backuper.py (the file you work with today) is not designed for tables with more than one field as a partition (if you tolerate multiple partitions of a single field), they are still cases to be evaluated.

The parameters that child receives:

 - Initial_load : receives by parameter True or False.
If True, it will generate a backup of the number of partitions that the productive table has from the most current, backwards, defined the number of partitions to save by max_partitions.
If it is False, it will generate a backup of the last partition of the productive table (if it does not have the same volume of records as the last partition loaded in the backup table), and the max_partitions parameter defines the limit of partitions that will be taken stored in the bkup table, dropping and deleting all those that exceed that limit (the order is from the most current to the oldest)
 - Max_partitions : receives an integer value as a parameter.
Defines the number of partitions to load initially for the case of Initial Load = True, or the number of partitions up to which will be saved in each table for the case of Initial Load = False.
 - Tables : receives a string of the tables to bkupear.

Examples:
 - If you load the day with parameters Initial_load = True, max_partitions = 4 and tables = jm_flows, it will generate a bkup table for us if it is not already created, and it will save the 4 most current partitions.
 - If you load day with parameters Initial_load = False, max_partitions = 2 and tables = jm_flows. Based on the previous example, we will already have the table created and with 4 partitions, but since we indicated that the maximum number of partitions be 2, it will delete the last 2 and leave us only the first 2.
 - If you load the day with parameters Initial_load = False, max_partitions = 3 and tables = jm_posic_contr, it will generate a bkup table if it is not created, and it will save only the last fraction (since initial_load is False), since it had no previous partitions, and we load only one partition, but we told the maximum number of partitions to be 3, it won't delete any partition because it doesn't meet the conditions.
The flowchart that roughly defines what the day does is as the one pdf loaded next to these files.
