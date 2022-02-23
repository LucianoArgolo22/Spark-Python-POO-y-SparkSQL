from pyspark.sql import SparkSession
import json
import sys
import os
import subprocess
import itertools

class SparkSessionYLogs:
	def loginfo(self):
		global log
		log = spark._jvm.org.apache.log4j.LogManager.getLogger('INFORMACIÓN DE USUARIO ----------------->>>>')
		level = spark._jvm.org.apache.log4j.Level.INFO
		log.setLevel(level)
		return log

	def get_contexts(self):
		spark = SparkSession.builder \
	        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
	        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
			.enableHiveSupport() \
			.getOrCreate()
		return spark

class CargaAbstracta:
	def __init__(self,tabla,partition = None):
		self.partition = partition
		self.tabla = tabla


	def get_contexts(self):
		spark = SparkSession.builder \
	        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
	        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
			.enableHiveSupport() \
			.getOrCreate()
		return spark


	def column_names(self):
		query = spark.sql(f"select * from bi_corp_bdr.{self.tabla} limit 0")
		return query.columns

	def colum_names_without_string(self):
		return ", ".join(self.column_names())

	def column_names_string(self):
		return ",".join([column + " string" for column in self.filtering_partition_from_column_names()])

	def filtering_partition_from_column_names(self):
		"""
		Filtra las particiones de los nombres de las columnas
		"""
		partition_columns = self.get_partition_names()
		return [column for column in self.column_names() if column not in partition_columns]

	def insert_partition(self):
		return ", ".join([partition.split("=")[0] + "=" + "'" + partition.split("=")[1] + "'" for partition in self.get_partition()])

	def insert_where(self, partition = -1, bkup = False, operator = "="):
		"""
		Valida que efectivamente exista la partición que se pide en "partition" como parámetro y se le resta 1 para que tenga valor de comparación menor al valor buscado (que se encuentre en el rango)
		En caso de que no haya tal partición, busca la última que exista
		"""
		log.info(f"Partición que Recibe insert_where(): {partition}")
		log.info(f"Validacion insert_where(), tabla {self.tabla}, bkup '{bkup}' : {self.validacion_tiene_particiones(cantidad = (partition - 1), bkup = bkup)}")
		if partition != -1:
			if self.validacion_tiene_particiones(cantidad = (partition - 1), bkup = bkup):
				return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(-1 * partition, bkup)])
			else:
				return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(partition=0, bkup=bkup)])
		else:
			return " and ".join([partition.split("=")[0] + operator + "'" + partition.split("=")[1] + "'" for partition in self.get_partition(partition=partition, bkup=bkup)])

	def tablas_DB(self):
		tablas = spark.sql("show tables in bi_corp_bdr").collect()
		return [tabla[1] for tabla in tablas]

	def tabla_in_DB(self):
		return tabla in self.tablas_DB()


	def get_partition_names(self):
		return list(self.partitions_dict().keys())

	def partition_string(self):
		"""
		genera un string como el EJEMPLO: "partition_date string, tabla string"
		sirve para crear tablas
		"""
		return ",".join([partition + " " + "string" for partition in self.get_partition_names()])

	def partition_without_string(self):
		"""
		genera un string a partir de las columnas de partición como

		EJEMPLO:  "partition_date , tabla "
		sirve para armar queries solo con las particiones
		"""
		return ", ".join([partition for partition in self.get_partition_names()])

	def select_partitions_string(self):
		"""
		Trae un string de las distintas particiones
		"""
		return f"select distinct {self.partition_without_string()} from bi_corp_bdr.{self.tabla}_bkup_dag order by {self.partition_without_string()} desc "

	def sort_partitions(self, limit = 1):	
		"""
		recibe por parámetro el límite de la cantidad de particiones, 
		y devuelve las mismas en una lista ordenadas de Mayor a menor a devolver
		Ej con limit por default (limit = 1)
		particiones ordenadas (3):  [Row(partition_date='2024-11-20'), Row(partition_date='2023-11-20'), Row(partition_date='2021-11-20')]	
		Ej con limit = 3
		particiones ordenadas (1):  [Row(partition_date='2024-11-20')]
		Obs: cada elemento, tiene en el valor las comillas colocadas
		"""
		return spark.sql(self.select_partitions_string() + f" limit {limit}").collect()[0:limit]

	def get_sort_partitions(self, limit = 1):
		"""
		Recibe por parámetro el límite de cantidad de elementos que devuelve la lista
		Ej con limite = 1 y limit = 3
		Get sort partitions (1): ['2024-11-20']
		Get sort partitions (3): ['2024-11-20', '2023-11-20', '2021-11-20']
		"""
		partitions = self.sort_partitions(limit)
		return list(itertools.chain(*[partition[0].split("/") for partition in partitions]))

	def get_partition(self, partition = -1, bkup = False):
		"""
		Obtiene las columnas correspondientes a las particiones de una sola partición, por default trae la última (ordenada o no)
		da la opción de traer partición del bkup o no 
		EJ: ["partition_date=2021-01-12", "tabla=prueba"]
		observar que dentro de cada elemento no hay comillas para cada valor, sino para el elemento entero
		"""
		log.info(f"Partición que recibe get_partition(): {partition}")
		tabla_partition = self.tabla + "_bkup_dag" if bkup else self.tabla	
		return self.partition_table(tabla_partition).collect()[partition][0].split("/")

	def partition_table(self, tabla_partition):
		return spark.sql(f"show partitions bi_corp_bdr.{tabla_partition}")

	def validacion_tiene_particiones(self, cantidad = 0, bkup = True):
		"""
		Valida que la tabla de bkup tenga particiones
		"""
		tabla_partition = self.tabla + "_bkup_dag" if bkup else self.tabla
		return len(self.partition_table(tabla_partition).collect()) > cantidad

	def validacion_volumen_registros_particiones(self):
		"""
		Devuelve True o False por la sentencia de la validación del volumen de las tablas
		"""
		return spark.sql(self.validacion_mismo_volumen_particion_previa()).collect()[0][0]

	def partitions_dict(self):
		"""
		crea un diccionario de particiones con sus valores correspondientes 
		EJ : {"partition_date": "2021-01-12","tabla": "prueba"}
		"""
		partition_dict = {}
		[partition_dict.update({str(part.split("=")[0]) : str(part.split("=")[1])}) for part in self.get_partition()]
		return partition_dict


class ArmadoDeQueries(CargaAbstracta):


	def insert_into_query(self):
		"""
		Genera la Query para hacer el insert
		"""
		string = f"""	                insert overwrite table bi_corp_bdr.{tabla}_bkup_dag 
						partition ({(self.insert_partition())}) 
						select {", ".join(self.filtering_partition_from_column_names())}
						from bi_corp_bdr.{self.tabla}
						where {self.insert_where()}
						"""
		log.info(f"La Query generada para insertar datos en la tabla backup de '{self.tabla}_bkup_dag' es: \n {string}")
		return string


	def create_table_query(self):
		"""
		Genera la Query para crear la tabla bkup en caso de que no exista
		"""
		partitions = self.partition_string() 
		string = f"""	                 create external table if not exists bi_corp_bdr.{self.tabla}_bkup_dag 
						({self.column_names_string()}) 
						partitioned by ({partitions})
						 stored as parquet 
						 location 'hdfs://namesrvprod/santander/bi-corp/bdr/bkup/bkup_dag/{self.tabla}_bkup_dag'"""
		log.info(f"La Query generada para crear la tabla backup de '{self.tabla}' es: \n {string}")
		return string



	def validacion_mismo_volumen_particion_previa(self):
		"""
		Validar si el contenido (lo hace por cantidad de registros únicamente) es el mismo que el de la última partición cargada
		por ende no volviendo a realizar otra carga, compara la última partición a cargar de la tabla prod, con la última partición de
		la tabla _backup_dag
		Devuelve: True o False
		"""
		return f"""select 
		                CASE 
		                        when campo1 = campo2 then False 
		                        else True
		                        end as cumple
		                from    (select 1 as id, count(*) as campo1 from bi_corp_bdr.{self.tabla} where {self.insert_where()}) a 
		                        inner join (select 1 as id, count(*) as campo2 from bi_corp_bdr.{self.tabla}_bkup_dag where {self.insert_where(bkup=True)}) b
		                        on  a.id = b.id	"""


	def sql_sql(self):
		
		if self.tabla_in_DB():
			log.info(f"La Tabla '{self.tabla}' se encuentra en la base de datos bi_corp_bdr")
			spark.sql(self.create_table_query())
			
			if self.validacion_tiene_particiones():
				log.info(f"La Tabla '{self.tabla}'_bkup_dag tiene particiones previas")
				
				if self.validacion_volumen_registros_particiones():
					log.info(f"La última partición de la tabla '{self.tabla}'_bkup_dag no posee el mismo volumen de datos que la última partición de la '{self.tabla}', se asume son distintas, se procede a cargarla")
					spark.sql(self.insert_into_query())
					log.info(f"TABLE {self.tabla}_bkup_dag LOADED")
				
				else:
					log.info(f"La última partición de la tabla '{self.tabla}'_bkup_dag posee el mismo volumen de datos que la última partición de la '{self.tabla}', se asume son iguales, no se carga")
					log.info(f"TABLE {self.tabla}_bkup_dag NOT LOADED, TIENEN MISMO VOLUMEN AMBAS PARTICIONES DE AMBAS TABLAS")
			
			else:
				log.info(f"La Tabla {self.tabla}_bkup_dag no tiene particiones previas")
				spark.sql(self.insert_into_query())
				log.info(f"TABLE {self.tabla}_bkup_dag LOADED")

class ArmadoDeQueriesCargaInicial(ArmadoDeQueries):

	def insert_into_query(self,partition):
		"""
		Genera una Query de carga inicial, que carga inicialmente una tabla vacía o recién creada con n particiones (partition )
		Se le pasa partition como parámetro, que va a buscar la partición en la ubicación solicitada (desde la última para la primera cronológicamente)
		por lo que si le paso partition = 3, va a buscar la partición - 3 en una lista como [0, 1, 2, 3], traería el "1"
		y luego carga a partir de ese valor, todos los superiores con el operador ">" del método insert_where
		"""
		log.info("Partición que recibe insert_into_query(): {partition}")
		string = f"""	                insert overwrite table bi_corp_bdr.{tabla}_bkup_dag 
						partition ({(self.partition_without_string())}) 
						select *
						from bi_corp_bdr.{self.tabla}
						where {self.insert_where(partition, operator = ">=")}
						"""
		log.info(f"La Query generada para insertar datos en la tabla backup de '{self.tabla}_bkup_dag' es: \n {string}")
		return string

	def sql_sql(self,partition):
		
		if self.tabla_in_DB():
			log.info(f"La Tabla '{self.tabla}' se encuentra en la base de datos bi_corp_bdr")
			spark.sql(self.create_table_query())
			spark.sql(self.insert_into_query(partition))
			log.info(f"TABLE {self.tabla}_bkup_dag LOADED")


class BorradoParticiones(CargaAbstracta):

	def filter_partitions_to_delete(self,max_partitions = 3):
		"""
		Recibe por parámetro una cantidad n de particiones, y se queda con n+1 particiones
		para posteriormente borrarlas
		devuelve una lista partida(respecto de la que recibe) en orden cronológico descendiente
		EJEMPLO: [Row(partition_date='2021-06'), Row(partition_date='2021-07'),
		 Row(partition_date='2021-08')]
		"""
		log.info(f"filter_partitions_to_delete -- max_partitions = {max_partitions}")
		spark.sql(self.select_partitions_string()).collect()[max_partitions:]
		return spark.sql(self.select_partitions_string()).collect()[max_partitions:]


	def partition_location(self,max_partitions = 3):
		"""
		Valida que tenga más de 'max_partitions' la tabla, antes de borrar las particiones que superen esa
		"""
		log.info("Partition_location -- Validando si el bkup tiene particiones")
		if self.validacion_tiene_particiones(cantidad = max_partitions):
			log.info("Partition_location -- True")
			filtered_partitions = self.filter_partitions_to_delete(max_partitions)
			return "\n".join([f"hdfs dfs -rm -r  /santander/bi-corp/bdr/bkup/bkup_dag/{self.tabla}_bkup_dag/{list(self.partitions_dict())[0]}={partition[0]}" for partition in filtered_partitions]) 
		
		else:
			log.info("Partition_location -- False")
			return " "

	def drop_partitions_query(self, max_partition_limit):
		log.info("Drop_partitions_query -- Validando si el bkup tiene particiones")

		if self.validacion_tiene_particiones(cantidad = max_partition_limit):
			queries = []
			log.info("Drop_partitions_query -- True")
			partitions_drop = self.filter_partitions_to_delete(max_partitions = max_partition_limit)
			log.info(f"Partitions_Drop: {partitions_drop}")
			partition_column = list(self.partitions_dict().keys())[0]
			for partition in partitions_drop:
				log.info(f"partición esperada?:{partition}")
				queries.append(f"ALTER TABLE bi_corp_bdr.{self.tabla}_bkup_dag DROP IF EXISTS PARTITION({partition_column}='{''.join(partition[0])}')")
			return queries
		else:
			log.info("Drop_partitions_query -- False")
			return ["select 'No hay particiones para dropear' as campo"]

	def drop_partitions(self,max_partition_limit = 3):
		query_dropeo = self.drop_partitions_query(max_partition_limit)
		log.info(f"Drop_partitions  Query:-- {query_dropeo}")
		for query in query_dropeo:
			spark.sql(query)


class ComandosBash:
	def __init__(self, filename, path="", mode = "w"  ):
		self.filename = filename
		self.path = path
		self.mode = mode
		self.string_to_write = []

	def append_strings(self,string_to_write):
		self.string_to_write.append(string_to_write)


	def bash_command(self,strings = " "):
		proc = subprocess.Popen([strings], stdout=subprocess.PIPE, shell=True)
		(out, err) = proc.communicate()
		out = out.decode("utf-8")
		return out.split("\n")

	def bash_already_exists(self):
		ls_grep_ComandosBash = " , ".join(self.bash_command(strings="hdfs dfs -ls /santander/bi-corp/bdr/bkup/bkup_files |grep ComandosBash.sh "))
		return "ComandosBash.sh" in  ls_grep_ComandosBash


	def read_write_file(self):
		if self.bash_already_exists():
			log.info(f"Read Write File -- Existe el archivo ComandosBash.sh, borrándolo")
			self.bash_command("hdfs dfs -rm /santander/bi-corp/bdr/bkup/bkup_files/ComandosBash.sh")
		with open(self.path + self.filename, self.mode) as f:
			log.info(f"Read Write File -- Creando archivo ComandosBash.sh ")
			if self.mode == "w":
				f.write("\n".join(self.string_to_write))
			else:
				pass
				#se puede agregar


if __name__ == "__main__":
	tablas = os.path.expandvars("$tablas").strip("[").strip("]").split(",")
	max_partitions = int(os.path.expandvars("$max_partitions"))
	carga_inicial = str(os.path.expandvars("$initial_load"))
	carga_inicial = True if carga_inicial == "True" or carga_inicial else False

	session_y_logs = SparkSessionYLogs()
	spark = session_y_logs.get_contexts()
	log = session_y_logs.loginfo()

	log.info(f"Initial load: {carga_inicial}")
	log.info(f"Tablas a backupear: {tablas}")
	log.info(f"Particiones Máximas : {max_partitions}")


	bash_generator = ComandosBash(filename = "ComandosBash.sh")
	for tabla in tablas:

		if carga_inicial:
			tabla = str(tabla)
			tabla_carga_inicial = ArmadoDeQueriesCargaInicial(tabla)	
			tabla_carga_inicial.sql_sql(max_partitions)

		else:
			tabla = str(tabla)
			tabla_bkup = ArmadoDeQueries(tabla)
			tabla_bkup.sql_sql()
			delete_partitions = BorradoParticiones(tabla)
			bash_generator.append_strings(string_to_write=delete_partitions.partition_location(max_partitions))
			delete_partitions.drop_partitions(max_partitions)

	bash_generator.append_strings(string_to_write="hdfs dfs -rm /santander/bi-corp/bdr/bkup/bkup_files/ComandosBash.sh")
	bash_generator.read_write_file()
	bash_generator.bash_command("hdfs dfs -put ComandosBash.sh /santander/bi-corp/bdr/bkup/bkup_files")

