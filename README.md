# Spark-Python-POO-and-SparkSQL
Automatic Loader that creates (if necessary) and load with parametry partitions of tables (it infers all the schema by itself).


Its purpose is to generate a backup of all the productive tables that are requested, with an X number of partitions, thus allowing the recovery of lost information in the event of an error.

IMPORTANT: The backuper.py (the file that the dag works with) is not designed for tables with more than one field as a partition (it tolerates having multiple partitions of a single field), these are still cases to be evaluated.

The parameters it receives are:

 - History_load : receives by parameter True or False.
If True, it will generate a backup of the number of partitions that the productive table has from the most current, backwards, defined the number of partitions to save by Load.
If it is False, it will generate a backup of the last partition of the productive table (if it does not have the same volume of records as the last partition loaded in the backup table), and the Max_over_delete parameter defines the limit of partitions to have stored in the bkup table, dropping and deleting all those that exceed that limit (the order is from the most current to the oldest)
 - Load : receives a value of type integer as a parameter.
Defines the number of partitions to load initially for the case of History_load = True.
 - Max_over_delete : receives a value of type integer as a parameter.
Defines the number of partitions to be deleted beyond the number of the Max_over_delete parameter in each table for the case of History_load = False.
 - Tables : receives a string of the tables to bkupear.

The use of "History_load" = True, is to initially load a batch of partitions in the table, because the False mode only loads the last partition (if it meets the conditions) and that it is loaded daily, or weekly with the History_load mode = False.

Examples:

 - If I load the dag with parameters History_load = True, Load = 3 and tables = jm_flujos_lucho (test table), it will generate a bkup table for us if it is not created, and it will save the 4 most current partitions (Recommended use to load a new table with the necessary partitions)
 - If I load the dag with parameters History_load = False, Max_over_delete = 0 and tables = jm_flujos_lucho (test table). Based on the previous example, we will already have the table created and with 4 partitions, but since we indicated that the maximum number of partitions be 2, it will delete the last 2 and leave us only the first 2.
 - If I load the dag with parameters History_load = False, Max_over_delete = 3 and tables = jm_posic_contr, it will generate a bkup table for us if it is not created, and it will save only the last partition (since initial_load is False), since it had no previous partitions, and we load only one partition, but we told the maximum number of partitions to be 3, it won't delete any partition because it doesn't meet the conditions.
 
 
## The Flux Diagram:
 ![descarga](https://user-images.githubusercontent.com/75091406/208456260-26b22ca5-1c7d-4975-88e1-95afbf459f65.png)

 
