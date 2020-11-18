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
df_ordenado = df.orderBy("update_date", ascending=False)


# ### Alterando os data types de acordo com o arquivo de mapeamento previamente carregado

# In[ ]:


# from pyspark.sql.functions import col, max as max_

df_transformado = df_ordenado.withColumn("update_date", col("update_date").cast(mappings.update_date))     .withColumn("create_date", col("create_date").cast(mappings.create_date))     .withColumn("age", col("age").cast(mappings.age))


# In[ ]:


df_transformado.printSchema()


# ### Retirando os dados deduplicados e ordenando o resultado por id para melhor visualização

# In[ ]:


df_transformado = df_transformado.dropDuplicates(["id"]).orderBy(["id"])


# ### Gravando o arquivo no formato parquet na pasta de saida

# In[ ]:


df_transformado.coalesce(1).write.mode('overwrite').parquet("data/output/cadastro/")


# In[ ]:


dt_parquet = spark.read.parquet('data/output/cadastro/')
dt_parquet.show()

