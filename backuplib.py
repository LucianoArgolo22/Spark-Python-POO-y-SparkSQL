from pyspark.sql import SparkSession
import os
import subprocess
import itertools

class SparkSessionYLogs:
	def loginfo(self):
		'''
		Creates a log for the spark session.
		:return: A log session.
		'''
		global log
		log = self.spark._jvm.org.apache.log4j.LogManager.getLogger('INFORMACIÓN DE USUARIO ----------------->>>>')
		level = self.spark._jvm.org.apache.log4j.Level.INFO
		log.setLevel(level)
		return log

	def get_contexts(self):
		'''
		Creates a spark session.
		:return: A spark session.
		'''
		print("""────────────────────██████──────────
──────────────────██▓▓▓▓▓▓██────────
────────────────██▓▓▓▓▒▒▒▒██────────
────────────────██▓▓▒▒▒▒▒▒██────────
──────────────██▓▓▓▓▒▒▒▒██──────────
──────────────██▓▓▒▒▒▒▒▒██──────────
────────────██▓▓▓▓▒▒▒▒▒▒██──────────
────────────████▒▒████▒▒██──────────
────────────██▓▓▒▒▒▒▒▒▒▒██──────────
──────────██────▒▒────▒▒██──────────
──────────████──▒▒██──▒▒██──────────
──────────██────▒▒────▒▒██──────────
──────────██▒▒▒▒▒▒▒▒▒▒▒▒██──────────
──────────████████████▒▒▒▒██────────
────────██▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒██──────
──────██▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒▒██────
────██▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒▒▒▒██──
──██▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒▒▒▒██
██▓▓▒▒▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒▒▒▒██
██▓▓▒▒▓▓▒▒▒▒▒▒▓▓▓▓▒▒▒▒▒▒▒▒▒▒▓▓▓▓▒▒██
██▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓██
──████▐▌▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▐▌▐▌████──
────██▐▌▐▌▌▌▌▌▌▌▌▌▐▌▐▌▐▌▐▌▌▌▐▌██────
────██▌▌▐▌▐▌▌▌▐▌▌▌▌▌▐▌▌▌▌▌▌▌▌▌██────
──────██▌▌▐▌▐▌▐▌▐▌▐▌▐▌▐▌▌▌▌▌██──────
──────██▐▌▐▌▐▌████████▐▌▌▌▌▌██──────
────────██▒▒██────────██▒▒██────────
────────██████────────██████────────""")
		spark = SparkSession.builder \
	        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
	        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
			.config("spark.dynamicAllocation.enabled", "true") \
			.enableHiveSupport() \
			.getOrCreate()
		self.spark = spark
		return spark

class CargaAbstracta:
	def __init__(self, tabla, spark, data_base, partition = None, limit_show = 10000):
		'''
		Constructor for the class.
		:param tabla: The dataset.
		:param partition: The partition of the dataset.
		'''
		self.partition = partition
		self.tabla = str(tabla)
		self.spark = spark
		self.data_base = data_base
		self.limit_show = limit_show


	def column_names(self):
		'''
		Returns the column names of the table.
		:return: A list of column names.
		'''
		query = self.spark.sql(f"select * from {self.data_base}.{self.tabla} limit 0")
		return query.columns

	def column_types(self):
		'''
		Returns the column types of the table.
		:return: A list of column types.
		'''
		columns_description = self.spark.sql("""describe X""").collect()
		return  [columns_description[i][1] for i in range(len(columns_description))]

	def colum_names_without_string(self):
		'''
		Returns the column names of the dataset.
		:return: A string of column names.
		'''
		return ", ".join(self.column_names())

	#------------------------

	def column_types(self):
		'''
		Returns the column types of the table.
		:return: A list of column types.
		'''
		columns_description = self.spark.sql(f"""describe {self.data_base}.{self.tabla}""").collect()
		return  [columns_description[i][1] for i in range(len(columns_description))]

	def column_types_filtered_partition(self, partitioned = True):
		'''
		Returns a string of the column names separated by a comma.
		:return: A string of the column names separated by a comma.
		'''
		partition_columns = self.get_partition_names() if partitioned else " "
		columns_description = self.spark.sql(f"""describe {self.data_base}.{self.tabla}""").collect()
		return  [columns_description[i][1] for i in range(len(columns_description)) if columns_description[i][0] not in partition_columns]

	def column_names_string(self, partitioned=True):
		'''
		Returns a string of the column names separated by a comma.
		:return: A string of the column names separated by a comma.
		'''
		columns, types = self.filtering_partition_from_column_names(partitioned=partitioned), self.column_types_filtered_partition(partitioned=partitioned)
		return ", ".join([columns[i] +"  "+  types[i] for i in range(len(columns))])
    #---------------------

	def filtering_partition_from_column_names(self, partitioned = True):
		'''
		Filters the partitions from de column names
		:return: a list of column names that without partition columns
		'''
		
		partition_columns = self.get_partition_names() if partitioned else [" "]
		return [column for column in self.column_names() if column not in partition_columns]



	def insert_partition(self):
		'''
		Inserts the partition into the query.
		:return: A string of the partition.
		'''

		return ", ".join([partition.split("=")[0] + "=" + "'" + partition.split("=")[1] + "'" for partition in self.get_partition()])

	def insert_where(self, partition = -1, bkup = False, operator = "="):
		'''
		This function is used to insert a where clause in a query.
		Validates that the partition requested in "partition" as a parameter actually exists 
		and 1 is subtracted so that it has a comparison value less than the value sought (which is in the range)
		In case there is no such partition, find the last one that exists
		:param partition: The partition to be inserted in the clause.
		:param bkup: If the partition is a backup partition.
		:param operator: The operator to be used in the clause.
		:return: A string with the and clause.
		'''
		if partition != -1:
			if self.validacion_tiene_particiones(cantidad = (partition - 1), bkup = bkup):
				return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(-1 * partition, bkup)])
			else:
				return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(partition=0, bkup=bkup)])
		else:
			return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(partition=partition, bkup=bkup)])


	def tablas_DB(self):
		'''
		Returns the names of the partitions in the database.
		:return: A list of partition names.
		'''
		tablas = self.spark.sql(f"show tables in {self.data_base}").collect()
		return [tabla[1] for tabla in tablas]

	def tabla_in_DB(self, bkup = False):
		'''
		Checks if the table is present in the database.
		:return: True if the table is present in the database, False otherwise.
		'''
		backup = self.tabla + "_bkup_dag" if bkup else self.tabla 
		log.info(f"tabla_in_DB -- Self.tabla + backup : { backup}")
		log.info(f"tabla_in_DB -- { backup in self.tablas_DB()}")
		return backup in self.tablas_DB()


	def get_partition_names(self):
		'''
		Returns the names of the partitions in the dataset.
		:return: A list of partition names.
		'''
		return list(self.partitions_dict().keys())

	def partition_string(self):
		'''
		Partitions the string into a list of characters.
		:return: A list of characters.
		Ex: "partition_date string, tabla string"
		it is used for create table queries
		'''
		return ",".join([partition + " " + "string" for partition in self.get_partition_names()])

	def partition_without_string(self):
		'''
		Partitions the dataset into training and testing sets.
		:return: Training and testing sets.
		ex:  "partition_date , tabla "
		it works to create querys only with partition names
		"""
		'''
		return ", ".join([partition for partition in self.get_partition_names()])

	def select_partitions_string(self):
		'''
		Selects the partitions of the dataset.
		:return: A list of the partitions.
		'''
		return f"select distinct {self.partition_without_string()} from {self.data_base}.{self.tabla}_bkup_dag order by {self.partition_without_string()} desc "

	def sort_partitions(self, limit = 1):	
		'''
		Sorts the partitions based on the number of rows in each partition.
		:return: A list of partitions sorted based on the number of rows in each partition.
		Ex with limit by default (limit = 1)
		partitions ordered (3):  [Row(partition_date='2024-11-20'), Row(partition_date='2023-11-20'), Row(partition_date='2021-11-20')]	
		Ex with limit = 3
		partitions ordered (1):  [Row(partition_date='2024-11-20')]
		Obs: Each element has the value with quotes in its current partition value
		'''

		return self.spark.sql(self.select_partitions_string() + f" limit {limit}").collect()[0:limit]

	def get_sort_partitions(self, limit = 1):
		'''
		This function returns the partitions of the dataset.
		:param limit: The number of partitions to be returned.
		:return: A list of the partitions.
		Ex with limit = 1 and limit = 3
		Get sort partitions (1): ['2024-11-20']
		Get sort partitions (3): ['2024-11-20', '2023-11-20', '2021-11-20']
		'''

		partitions = self.sort_partitions(limit)
		return list(itertools.chain(*[partition[0].split("/") for partition in partitions]))

	def get_partition(self, partition = -1, bkup = False):
		'''
		Gets the partition that is going to be processed.
		:param partition: The partition that is going to be processed.
		:param bkup: If the partition is from the backup table.
		:return: The partition that is going to be processed.
		EX: ["partition_date=2021-01-12", "tabla=prueba"]
		notice that within each element there are no quotes for each value, but for the entire element
		'''
		tabla_partition = self.tabla + "_bkup_dag" if bkup else self.tabla	
		log.info(f"Partición que recibe get_partition(): {self.partition_table(tabla_partition).collect()[partition][0]}")
		return self.partition_table(tabla_partition).collect()[partition][0].split("/")

	def partition_table(self, tabla_partition):
		'''
		This function is used to partition a table in a database.
		:param tabla_partition: The table to be partitioned.
		:return: The partitions of the table.
		'''
		return self.spark.sql(f"show partitions {self.data_base}.{tabla_partition}")

	def validacion_tiene_particiones(self, cantidad = 0, bkup = True):
		'''
		Checks if the table has partitions.
		:param cantidad: The number of partitions to check for.
		:param bkup: Whether to check for the backup table or the main table.
		:return: True if the table has at least the given number of partitions.
		'''
		tabla_partition = self.tabla + "_bkup_dag" if bkup else self.tabla
		return len(self.partition_table(tabla_partition).collect()) > cantidad

	def validacion_volumen_registros_particiones(self):
		'''
		Validates if the number of rows in each partition is the same as the number of rows in the previous partition.
		:return: A boolean value.
		'''
		return self.spark.sql(self.validacion_mismo_volumen_particion_previa()).collect()[0][0]

	def partitions_dict(self):
		'''
		Creates a dictionary of the partitions of the dataset.
		:return: A dictionary of the partitions of the dataset.
		Ex : {"partition_date": "2021-01-12","tabla": "prueba"}
		'''
		partition_dict = {}
		[partition_dict.update({str(part.split("=")[0]) : str(part.split("=")[1])}) for part in self.get_partition()]
		return partition_dict


class ArmadoDeQueries(CargaAbstracta):


	def insert_into_query(self):
		'''
		Generates a query to insert data into the backup table.
		:return: A string containing the query.
		'''
		string = f"""	                insert overwrite table {self.data_base}.{self.tabla}_bkup_dag 
						partition ({(self.insert_partition())}) 
						select {", ".join(self.filtering_partition_from_column_names())}
						from {self.data_base}.{self.tabla}
						where {self.insert_where()}
						"""
		log.info(f"La Query generada para insertar datos en la tabla backup de '{self.tabla}_bkup_dag' es: \n {string}")
		return string


	def create_table_query(self):
		'''
		Creates a query to create a backup table for the given table.
		:return: A string containing the query.
		'''
		partitions = self.partition_string() 
		string = f"""	                 create external table if not exists {self.data_base}.{self.tabla}_bkup_dag 
						({self.column_names_string()}) 
						partitioned by ({partitions})
						 stored as parquet 
						 location 'hdfs://path/to/{self.data_base}/{self.tabla}_bkup_dag'"""
		log.info(f"La Query generada para crear la tabla backup de '{self.tabla}' es: \n {string}")
		return string



	def validacion_mismo_volumen_particion_previa(self):
		'''
		This function checks if the volume of the partition is the same as the previous partition.
		:return: A query that returns a boolean value.
		'''
		return f"""select 
		                CASE 
		                        when campo1 = campo2 then False 
		                        else True
		                        end as cumple
		                from    (select 1 as id, count(*) as campo1 from {self.data_base}.{self.tabla} where {self.insert_where()}) a 
		                        inner join (select 1 as id, count(*) as campo2 from {self.data_base}.{self.tabla}_bkup_dag where {self.insert_where(bkup=True)}) b
		                        on  a.id = b.id	"""


	def sql_sql(self):
		'''
		This function checks if the table is in the database,
		if it is, it checks if it has partitions,
		if it has, it checks if the last partition has the same volume of data as the last partition of the table,
		if it does, it does not load the table, if it does not, it loads it.
		:return: None
		'''
			
		if self.tabla_in_DB():
			log.info(f"La Tabla '{self.tabla}' se encuentra en la base de datos {self.data_base}")
			
			if self.validacion_tiene_particiones(bkup=False):
				self.spark.sql(self.create_table_query())
				log.info(f"La Tabla '{self.tabla}' tiene particiones")
				if self.validacion_tiene_particiones():
					log.info(f"La Tabla '{self.tabla}'_bkup_dag tiene particiones previas")
					
					if self.validacion_volumen_registros_particiones():
						log.info(f"La última partición de la tabla '{self.tabla}'_bkup_dag no posee el mismo volumen de datos que la última partición de la '{self.tabla}', se asume son distintas, se procede a cargarla")
						self.spark.sql(self.insert_into_query())
						log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
					
					else:
						log.info(f"La última partición de la tabla '{self.tabla}'_bkup_dag posee el mismo volumen de datos que la última partición de la '{self.tabla}', se asume son iguales, no se carga")
						log.info(f"TABLE {self.tabla}_bkup_dag NOT LOADED, TIENEN MISMO VOLUMEN AMBAS PARTICIONES DE AMBAS TABLAS")
				
				else:
					log.info(f"La Tabla {self.tabla}_bkup_dag no tiene particiones previas")
					self.spark.sql(self.insert_into_query())
					log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
			else:
				log.info(f"La Tabla {self.tabla} no tiene particiones")

class ArmadoDeQueriesCargaInicial(ArmadoDeQueries):

	def insert_into_query(self,partition):
		'''
		Generates a query to insert data into the backup table.
		:param partition: The partition that is being inserted into the table.
		:return: A string containing the query to be executed.
		'''
		log.info("Partición que recibe insert_into_query(): {partition}")
		string = f"""	                insert overwrite table {self.data_base}.{self.tabla}_bkup_dag 
						partition ({(self.partition_without_string())}) 
						select *
						from {self.data_base}.{self.tabla}
						where {self.insert_where(partition, operator = ">=")}
						"""
		log.info(f"La Query generada para insertar datos en la tabla backup de '{self.tabla}_bkup_dag' es: \n {string}")
		return string


	def sql_sql(self,partition):
		'''
		This function checks if the table is already in the database. If it is, it creates a backup table.
		:param partition: The partition of the table to be loaded.
		:return: None
		'''
		if self.tabla_in_DB() :
			if self.validacion_tiene_particiones(bkup=False):
				log.info(f"La Tabla '{self.tabla}' se encuentra en la base de datos {self.data_base}")
				self.spark.sql(self.create_table_query())
				self.spark.sql(self.insert_into_query(partition))
				log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
			else:
				log.info(f"La Tabla '{self.tabla}' NO tiene datos para cargar")
		else:
			log.info(f"La Tabla '{self.tabla}' NO se encuentra en la base de datos {self.data_base}")



class ArmadoDeQueriesNoPartition(ArmadoDeQueriesCargaInicial):	
	def validacion_mismo_volumen_particion_previa(self):
		'''
		This function checks if the volume of the partition is the same as the previous partition.
		:return: A query that returns a boolean value.
		'''
		return f"""select 
						CASE 
								when campo1 = campo2 then False 
								else True
								end as cumple
						from    (select 1 as id, count(*) as campo1 from {self.data_base}.{self.tabla} ) a 
								inner join (select 1 as id, count(*) as campo2 from {self.data_base}.{self.tabla}_bkup_dag) b
								on  a.id = b.id	"""


	def create_table_query(self):
		'''
		Creates a query to create a backup table for the given table.
		:return: A string containing the query.
		'''
		string = f"""	                 create external table if not exists {self.data_base}.{self.tabla}_bkup_dag 
						({self.column_names_string(partitioned=False)}) 
							stored as parquet 
							location 'hdfs://path/to/{self.data_base}/{self.tabla}_bkup_dag'"""
		log.info(f"La Query generada para crear la tabla backup de '{self.tabla}' es: \n {string}")
		return string

	def insert_into_query(self):
		'''
		Generates a query to insert data into the backup table.
		:param partition: The partition that is being inserted into the table.
		:return: A string containing the query to be executed.
		'''
		string = f"""	                insert overwrite table {self.data_base}.{self.tabla}_bkup_dag 
						select *
						from {self.data_base}.{self.tabla}
						"""
		log.info(f"La Query generada para insertar datos en la tabla backup de '{self.tabla}_bkup_dag' es: \n {string}")
		return string

	def sql_sql(self):
		'''
		This function checks if the table is already in the database. If it is, it creates a backup table.
		:param partition: The partition of the table to be loaded.
		:return: None
		'''
		if self.tabla_in_DB():
			log.info(f"La Tabla '{self.tabla}' se encuentra en la base de datos {self.data_base}")
			if self.tabla_in_DB(bkup=True):
				log.info(f"La Tabla '{self.tabla}_bkup_dag' se encuentra en la base de datos {self.data_base}")
				if self.validacion_volumen_registros_particiones():
					log.info(f"La Tabla '{self.tabla}' tiene particiones")
					self.spark.sql(self.insert_into_query())
					log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
				else:
					log.info(f"La Tabla '{self.tabla}' Tiene el mismo volumen, se asumen datos iguales")
			else:
				log.info(f"La Tabla '{self.tabla}_bkup_dag' NO se encuentra en la base de datos {self.data_base}")
				self.spark.sql(self.create_table_query())
				self.spark.sql(self.insert_into_query())
				log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
		else:
			log.info(f"La Tabla '{self.tabla}' NO se encuentra en la base de datos {self.data_base}")


class Validador(CargaAbstracta):
	def conteo_(self, working_day):
		if self.tabla_in_DB():
			if self.validacion_esta_particionada():
				log.info(f"Particiones en {self.tabla}: {[partition[0].split('=')[1] for partition in self.spark.sql(f'show partitions {self.data_base}.{self.tabla}').collect()[::-1]]}")
				return self.spark.sql(f"select count(*) from {self.data_base}.{self.tabla} where partition_date='{working_day}'").collect()[0][0]
			else:
				log.info(f"Tabla {self.tabla} sin particionar")
				return self.spark.sql(f"select count(*) from {self.data_base}.{self.tabla} ").collect()[0][0]
		else:
			print(f"La tabla {self.tabla} no se encuentra en la base de datos {self.data_base}")
			return 0
	
	def validacion_esta_particionada(self):
		try:
			self.spark.sql(f"show partitions {self.data_base}.{self.tabla}")
			has_partitions = True
		except:
			has_partitions = False
		return has_partitions
			
	def creador_join(self, campo, i):
		if i == 0:
				return f"on coalesce(aux2.{campo}, 'null') = coalesce(aux1.{campo}, 'null')"
		return f"and coalesce(aux2.{campo}, 'null') = coalesce(aux1.{campo}, 'null')"

	def generador_string(self, n):
		last_string  = "  ".join([self.creador_join(campo, i) for i, campo in enumerate(self.column_names())]) 
		return last_string + f" where aux{n}.{self.column_names()[0]} is NULL" 
		 
	def where_partition(self): 
		if self.validacion_esta_particionada():
			return f"where partition_date = '{self.partition}'"
		else:
			return " "

	def with_tables(self):
		string=f"""with tabla_aux as (
			select * from {self.data_base}.{self.tabla}
			{self.where_partition()}
			),
			tabla_aux_bkup_dag as (
			select * from {self.data_base}.{self.tabla}_bkup_dag
			{self.where_partition()}
			)"""
		return string	

	def query_atributos(self, n, tabla = "TABLA PROD", with_tables_optional = True):
		if with_tables_optional == True:
			with_tables = self.with_tables()
		else:
			with_tables = " "
		return f"""{with_tables}
		select '{tabla}' as tabla, aux{n}.* from tabla_aux_bkup_dag aux2
		FULL JOIN tabla_aux aux1 """

	def final_query_to_execute(self, tablas_a_traer):
		if tablas_a_traer=='prod':
			query = self.query_atributos(n=1, tabla= "TABLA PROD", with_tables_optional = True).replace('\n','') + self.generador_string(n = 2)
		elif tablas_a_traer=='bkup':
			query = self.query_atributos(n=2, tabla= "TABLA BKUP", with_tables_optional = True).replace('\n','') + self.generador_string(n = 1)
		log.info(f"La query que ejecuta la validación de atributos es: {query} ")
		return query

	def sql_sql(self, tablas_a_traer): 
		return self.spark.sql(self.final_query_to_execute(tablas_a_traer))

	def union_all_string(self, campo, i):
		return f"""(select 1 as id, 'TABLA PROD' as tabla, count(distinct({campo})) as {campo} from  tabla_aux
			union all
			select 2 as id, 'TABLA BKUP' as tabla, count(distinct({campo})) as {campo} from  tabla_aux_bkup_dag ) a{i}"""

	def creador_join_distintos(self ,campos, campo, i):
		if i == 0:
			return f"""select  a0.tabla, {", ".join(self.column_names())} from 
			{self.union_all_string(campo, i)}"""
		return f"""inner join
			{self.union_all_string(campo, i)}
			on a{i}.id = a{int(i)-1}.id """

	def unir_queries(self):
		query = ""
		for i, campo in enumerate(self.column_names()):
			query += " " + self.creador_join_distintos(self.column_names(), campo, i )
		return  self.with_tables() + query


	def valores_distintos_sql(self):
			log.info(f"Resultado de unir queries {self.unir_queries()}")
			self.spark.sql(self.unir_queries()).show(truncate = False)


class BorradoParticiones(CargaAbstracta):

	def filter_partitions_to_delete(self,Load = 3):
		"""
		Recibe por parámetro una cantidad n de particiones, y se queda con n+1 particiones
		para posteriormente borrarlas
		devuelve una lista partida(respecto de la que recibe) en orden cronológico descendiente
		EJEMPLO: [Row(partition_date='2021-06'), Row(partition_date='2021-07'),
		 Row(partition_date='2021-08')]
		"""
		log.info(f"filter_partitions_to_delete -- Load = {Load}")
		filtered_partitions_to_delete = self.spark.sql(self.select_partitions_string()).collect()
		log.info(f"filter_partitions_to_delete -- filtered_partitions_to_delete = {filtered_partitions_to_delete}")
		log.info(f"filter_partitions_to_delete -- filtered_partitions_to_delete[Load:] = {filtered_partitions_to_delete[Load:]}")
		return filtered_partitions_to_delete[Load:]


	def partition_location(self,Load = 3):
		'''
		This function checks if the backup has partitions.
		:param Load: The maximum number of partitions to be retained.
		:return: A string with the commands to delete the partitions.
		'''
		log.info("Partition_location -- Validando si el bkup tiene particiones")
		if self.tabla_in_DB(bkup=True):
			if self.validacion_tiene_particiones(cantidad = Load):
				log.info("Partition_location -- True")
				filtered_partitions = self.filter_partitions_to_delete(Load)
				return "\n".join([f"hdfs dfs -rm -r  /path/to/{self.data_base}/{self.tabla}_bkup_dag/{list(self.partitions_dict())[0]}={partition[0]}" for partition in filtered_partitions]) 
			
			else:
				log.info("Partition_location -- False")
				return " "
		else:
			log.info("Partition_location, No bkup table found -- False")
			return " "

	def drop_partitions_query(self, max_partition_limit):
		'''
		This function checks if the backup table has partitions.
		:param max_partition_limit: The maximum number of partitions to be retained.
		:return: A list of queries to drop the partitions.
		'''
		log.info("Drop_partitions_query -- Validando si el bkup tiene particiones")

		if self.validacion_tiene_particiones(cantidad = max_partition_limit):
			queries = []
			log.info("Drop_partitions_query -- True")
			partitions_drop = self.filter_partitions_to_delete(Load = max_partition_limit)
			log.info(f"Partitions_Drop: {partitions_drop}")
			partition_column = list(self.partitions_dict().keys())[0]
			for partition in partitions_drop:
				log.info(f"partición esperada?:{partition}")
				queries.append(f"ALTER TABLE {self.data_base}.{self.tabla}_bkup_dag DROP IF EXISTS PARTITION({partition_column}='{''.join(partition[0])}')")
			return queries
		else:
			log.info("Drop_partitions_query -- False")
			return ["select 'No hay particiones para dropear' as campo"]

	def drop_partitions(self,max_partition_limit = 3):
		'''
		Drops the partitions of the table if they are greater than the max_partition_limit.
		:param max_partition_limit: The maximum number of partitions to be retained.
		:return: None
		'''
		if self.tabla_in_DB(bkup=True):
			query_dropeo = self.drop_partitions_query(max_partition_limit)
			log.info(f"Drop_partitions  Query:-- {query_dropeo}")
			for query in query_dropeo:
				self.spark.sql(query)


class ComandosBash:
	def __init__(self, filename, path="", mode = "w"  ):
		'''
		Constructor for the filewriter class.
		:param filename: Name of the file to be written.
		:param path: Path of the file to be written.
		:param mode: Mode of the file to be written.
		'''
		self.filename = filename
		self.path = path
		self.mode = mode
		self.string_to_write = []

	def append_strings(self,string_to_write):
		'''
		Appends a string to the list of strings.
		:param string_to_write: The string to be appended.
		:return: Nothing.
		'''
		self.string_to_write.append(string_to_write)

	@staticmethod
	def bash_command(strings = " "):
		'''
		This function executes bash commands.
		:return: The output of the bash command.
		'''
		proc = subprocess.Popen([strings], stdout=subprocess.PIPE, shell=True)
		(out, err) = proc.communicate()
		out = out.decode("utf-8")
		return out.split("\n")



	def read_write_file(self):
		'''
		Reads and writes a file.
		:param self: Self instance of the class.
		:param mode: The mode in which the file is to be opened.
		:return: None
		'''
		with open(self.path + self.filename, self.mode) as f:
			log.info(f"Read Write File -- Creando archivo Bash")
			strings = '\n'.join(self.string_to_write)
			log.info(f"""Read Write File -- Las particiones a borrar son: {strings}""")
			if self.mode == "w":
				f.write(strings)
			else:
				pass
				#se puede agregar
