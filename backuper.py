import os
from backuplib import *


if __name__ == "__main__":
	tablas = [tabla.strip() for tabla in os.path.expandvars("$tablas").split(",")]
	Partitions_to_preserve = int(os.path.expandvars("$Partitions_to_preserve"))
	Load = int(os.path.expandvars("$Load"))
	data_base = os.path.expandvars("$data_base")
	History_load = str(os.path.expandvars("$History_load"))
	History_load = True if History_load == "True" or History_load == True else False

	session_y_logs = SparkSessionYLogs()
	spark = session_y_logs.get_contexts()
	log = session_y_logs.loginfo()


	log.info(f"Archivos dentro del contenedor de Spark: {os.listdir()}")
	log.info(f"Archivos Python subidos: {os.listdir('__pyfiles__')}")

	log.info(f"Initial load: {History_load}")
	log.info(f"Data Base: {data_base}")
	log.info(f"Tablas a backupear: {tablas}")
	log.info(f"Particiones a preservar : {Partitions_to_preserve}")

	spark.sql("set mapred.job.queue.name=root.dataeng")
	log.info("Mapred job Queue: root.dateng")

	bash_generator = ComandosBash(filename = f"ComandosBash_{data_base}.sh")


	for tabla in tablas:
		validador_ = Validador(tabla, spark, data_base)
		has_partitions = validador_.validacion_esta_particionada()
		print(f"Tabla --> {tabla}, has_partitions --> {has_partitions}")

		if has_partitions:
			log.info(f"has_partitions = True, Tabla {tabla} tiene particiones")
			if History_load:
				tabla_History_load = ArmadoDeQueriesCargaInicial(tabla, spark, data_base)
				tabla_History_load.sql_sql(Load)

			else:
				if Partitions_to_preserve != 0:
					tabla_bkup = ArmadoDeQueries(tabla, spark, data_base)
					tabla_bkup.sql_sql()
				delete_partitions = BorradoParticiones(tabla, spark, data_base)
				bash_generator.append_strings(string_to_write=delete_partitions.partition_location(Partitions_to_preserve))
				delete_partitions.drop_partitions(Partitions_to_preserve)

		else:
			log.info(f"Tabla {tabla} NO tiene particiones")
			tabla_History_load = ArmadoDeQueriesNoPartition(tabla, spark, data_base)
			tabla_History_load.sql_sql()
		
	bash_generator.append_strings(string_to_write=f"hdfs dfs -rm  /path/to/bkup_files/ComandosBash_{data_base}.sh")
	bash_generator.read_write_file()
	bash_generator.bash_command(f"hdfs dfs -put -f ComandosBash_{data_base}.sh /path/to/bkup_files")
	
