#A validator that validates differences between tables that are bkup_dag and it's original table,
#1) it validates attributes.
#2) it validates volume of the data in each one.
#3) it validates if there is any differences between tables, wich rows and how many.
#4) the fourth one, brings the schema of each table, so one can see if the bkup_dag table has been generated properly


from backuplib import *
import os


if __name__ == "__main__":
	tablas = [tabla.strip().lower() for tabla in os.path.expandvars("$tablas").split(",")]
	data_base = os.path.expandvars("$data_base")
	partition = os.path.expandvars("$partition")
	limit_show = int(os.path.expandvars("$limit_show"))
	tablas_a_traer = os.path.expandvars("$tablas_a_traer")
	cantidad_particiones = os.path.expandvars("$cantidad_particiones")
	cantidad_particiones = cantidad_particiones if cantidad_particiones != "$cantidad_particiones" else -1 



	session_y_logs = SparkSessionYLogs()
	spark = session_y_logs.get_contexts()
	log = session_y_logs.loginfo()

	log.info(f"Archivos dentro del contenedor de Spark: {os.listdir()}")
	log.info(f"Archivos Python subidos: {os.listdir('__pyfiles__')}")


	acum = 0
	for tabla in tablas:
		#PARTE 1 VALIDANDO ATRIBUTOS
		print("---------------------PARTE 1 VALIDANDO ATRIBUTOS --------------")
		validador_tabla = Validador(tabla, spark, data_base, partition, limit_show)
		print(f"Validando Atributos de la Tabla {tabla}")
		print(f"Validando particion {partition}")
		if tablas_a_traer == "ambas":
			df1 = validador_tabla.sql_sql(tablas_a_traer = "prod")
			df1.show(n=limit_show, truncate=False)
			df2 = validador_tabla.sql_sql(tablas_a_traer = "bkup")
			df2.show(n=limit_show, truncate=False)
			print(f"Filas con atributos distintos PROD: {df1.count()}")
			print(f"Filas con atributos distintos BKUP: {df2.count()}")
		else:
			df = validador_tabla.sql_sql(tablas_a_traer = tablas_a_traer)
			df.show(n=limit_show,truncate=False)
			print(f"Filas con atributos distintos {tablas_a_traer.upper()}: {df.count()}")
		validador_tabla.valores_distintos_sql()



		print("---------------------PARTE 2 VALIDANDO VOLUMEN --------------")
		#PARTE 2 VALIDANDO VOLUMEEEN
		acum = 0
		acum_bkup = 0

		try:
			particiones = spark.sql(f"show partitions {data_base}.{tabla}").collect()[::-1][0:int(cantidad_particiones)]
			print(f"Particiones en {tabla}: {[partition[0].split('=')[1] for partition in spark.sql(f'show partitions {data_base}.{tabla}').collect()[::-1]][0:int(cantidad_particiones)]}")  #{[partition[0].split('=')[1] for partition in self.spark.sql(f'show partitions {self.data_base}.{self.tabla}').collect()[::-1]]}")
	
		except:
			particiones = [["tabla=no_está_particionada"]]

		for particion in particiones:
			particion = particion[0].split('=')[1]
			tabla_bkup = tabla + "_bkup_dag"
			print(f"Tabla actual: {tabla}")
			print(f"\nRecorriendo partición {particion}")
			validador_tabla = Validador(tabla, spark, data_base)
			conteo = validador_tabla.conteo_(particion)
			acum += 1 if conteo > 0 else 0
			#si la partición de la tabla original existe y tiene datos, entonces que se fije en la bkup
			if conteo != 0:
				validador_tabla_bkup = Validador(tabla_bkup, spark, data_base)
				conteo_bkup = validador_tabla_bkup.conteo_(particion)
				acum_bkup += 1 if conteo_bkup > 0 else 0
			else:
				conteo_bkup = 0
			print(f"\nConteo de datos de la tabla: {conteo}")
			print(f"Conteo de datos de la tabla BKUP para la misma partición: {conteo_bkup}")


		print("---------------------PARTE 3 CONTROL DIFERENCIAL--------------")

		#PARTE 3 CONTROL DIFERENCIAL
		print(f"Tabla: {tabla}")
		cuenta_comun = spark.sql(f"Select count(*) from {data_base}.{tabla} where partition_date = '{partition}'")
		print("conteo tabla original",cuenta_comun.collect()[0][0])
		cuenta_union = spark.sql(f"""select count(*) 
			from (Select * from {data_base}.{tabla} where partition_date = '{partition}'
			Union
			Select * from {data_base}.{tabla}_bkup_dag where partition_date = '{partition}')a""")
		print("conteo tablas unificadas",cuenta_union.collect()[0][0])
		df_original = spark.sql(f"Select * from {data_base}.{tabla} where partition_date = '{partition}'")
		df_bkup = spark.sql(f"Select * from {data_base}.{tabla}_bkup_dag where partition_date = '{partition}'")
		diff = df_original.subtract(df_bkup)
		print("diferencias:", diff.count())
		diff.show()


		print("---------------------PARTE 4 ESQUEMA DE LAS TABLAS--------------")
		#PARTE 4 FORMATOS DE LAS TABLAS
		print(f"\nESQUEMA DE LA TABLA {tabla}")
		spark.sql(f"describe {data_base}.{tabla}").show(truncate=False)
		print(f"\nESQUEMA DE LA TABLA {tabla}_bkup_dag")
		spark.sql(f"describe {data_base}.{tabla}_bkup_dag").show(truncate=False)

		print("---------------------------------------------------------------")
		print("---------------------------------------------------------------")
		print("---------------------------------------------------------------\n\n\n")
