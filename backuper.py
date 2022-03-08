
import os
from backuplib import *


if __name__ == "__main__":
	tablas = [tabla.strip() for tabla in os.path.expandvars("$tablas").split(",")]
	Max_over_delete = int(os.path.expandvars("$Max_over_delete"))
	Load = int(os.path.expandvars("$Load"))
	History_load = str(os.path.expandvars("$History_load"))
	History_load = True if History_load == "True" or History_load == True else False
	data_base = str(os.path.expandvars("$data_base"))
	
	session_y_logs = SparkSessionYLogs()
	spark = session_y_logs.get_contexts()
	log = session_y_logs.loginfo()

	log.info(f"Archivos dentro del contenedor de Spark: {os.listdir()}")
	log.info(f"Archivos Python subidos: {os.listdir('__pyfiles__')}")

	log.info(f"Initial load: {History_load}")
	log.info(f"Tablas a backupear: {tablas}")
	log.info(f"Particiones Máximas : {Load}")


	bash_generator = ComandosBash(filename = "ComandosBash.sh")


	for tabla in tablas:

		if History_load:
			tabla = str(tabla)
			tabla_History_load = ArmadoDeQueriesCargaInicial(tabla, spark, data_base)	
			tabla_History_load.sql_sql(Load)

		else:
			tabla = str(tabla)
			tabla_bkup = ArmadoDeQueries(tabla, spark, data_base)
			tabla_bkup.sql_sql()
			delete_partitions = BorradoParticiones(tabla, spark, data_base)
			bash_generator.append_strings(string_to_write=delete_partitions.partition_location(Max_over_delete))
			delete_partitions.drop_partitions(Max_over_delete)

	bash_generator.read_write_file()
	bash_generator.bash_command("hdfs dfs -put -f ComandosBash.sh path/to/bkup_files")
