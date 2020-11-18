#!/usr/bin/env python
# coding: utf-8

# ### Importando a biblioteca pyspark e criando uma sessão e contexto do spark para utilização

# In[ ]:


from pyspark.sql import SparkSession

spark = SparkSession     .builder     .appName("etl")     .getOrCreate()


# ### Importando o arquivo de mapeamento para conversão de data types

# In[ ]:


mappings = spark.read.option("multiline","true").json('config/types_mapping.json')


# In[ ]:


mappings.show()


# In[ ]:


mappings = mappings.select("age", "create_date", "update_date").first()


# ### Importando o arquivo .csv de input

# In[ ]:


df = spark.read.csv('data/input/load.csv', header='true')


# ### Ordenando o dataframe para Utilizar o update_date como filtro para obter os dados mais novos

# In[ ]:


from pyspark.sql.functions import col, max as max_
df_ordenado = df.withColumn("update_date", col("update_date").cast("timestamp")).groupBy("id", "name", "email", "phone", "address", "age", "create_date", "update_date").agg(max_("update_date"))


# ### Coletando apenas o ultimo registro de cada ID com o filtro de data mais recente da coluna Update_date e convertendo os datatypes fornecidos no arquivo JSON de mapeamento que foi carregado no inicio do processo.

# In[ ]:


from pyspark.sql.functions import col, max as max_

df_transformado = df_ordenado.withColumn("update_date", col("update_date").cast(mappings.update_date))     .withColumn("create_date", col("create_date").cast(mappings.create_date))     .withColumn("age", col("age").cast(mappings.age))     .groupBy("id", "name", "email", "phone", "address", "age", "create_date").agg(max_("update_date"))     .dropDuplicates(["id"]).orderBy("id")


# In[ ]:


df_transformado.printSchema()


# ### Renomeando a coluna para update_date

# In[ ]:


df_transformado = df_transformado.withColumnRenamed("max(update_date)", "update_date")


# ### Gravando o arquivo no formato parquet na pasta de saida

# In[ ]:


df_transformado.coalesce(1).write.mode('overwrite').parquet("data/output/cadastro/")


# In[ ]:


dt_parquet = spark.read.parquet('data/output/cadastro/')
dt_parquet.show()

