import tuplex
from mpi4py import MPI

# Inicializar el entorno MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Crear el contexto Tuplex en el nodo maestro (rango 0)
if rank == 0:
    c = tuplex.Context({'tuplex.redirectToPythonLogging': True, 'tuplex.executorMemory': '3G', 'tuplex.driverMemory': '3G'})
else:
    c = None

# Comunicar el contexto a todos los nodos
c = comm.bcast(c, root=0)

# Cargar el archivo CSV en el nodo maestro
if rank == 0:
    ds = c.csv('s3://clustertuplexproofs231/311_subset.csv')
else:
    ds = None

# Distribuir el conjunto de datos entre los nodos utilizando scatter
ds = comm.scatter(ds, root=0)

# Definir las funciones de extracción de mes y año
def extract_month(row):
    date = row['Created Date']
    date = date[:date.find(' ')]
    return int(date.split('/')[0])

def extract_year(row):
    date = row['Created Date']
    date = date[:date.find(' ')]
    return int(date.split('/')[-1])

# Realizar las transformaciones en el conjunto de datos
ds = ds.withColumn('Month', extract_month) \
       .withColumn('Year', extract_year) \
       .filter(lambda row: 'Mosquito' in row['Complaint Type']) \
       .filter(lambda row: row['Year'] == year_to_investigate) \
       .selectColumns(['Month', 'Year', 'Complaint Type'])

# Recopilar los resultados en el nodo maestro utilizando gather
results = comm.gather(ds, root=0)

# Mostrar los resultados en el nodo maestro (rango 0)
if rank == 0:
    # Concatenar los resultados en un solo conjunto de datos
    final_result = results[0].unionAll(results[1:])

    # Definir las funciones de combinación y agregación
    def combine_udf(a, b):
        return a + b

    def aggregate_udf(agg, row):
        return agg + 1

    # Realizar la agregación por clave y mostrar los resultados
    final_result.aggregateByKey(combine_udf, aggregate_udf, 0, ["Month"]).show()
    