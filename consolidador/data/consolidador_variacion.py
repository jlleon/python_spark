# coding=utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from configparser import ConfigParser # se debe cambiar de configparse a ConfigParser en produccion
import datetime
import os


class ConsolidadorVariacion:
    def __init__(self, fecha):
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

        config = ConfigParser()
        config.read(os.path.join(ROOT_DIR, 'data_source.ini'))
        master_url = config.get('spark', 'master_url')
        app_name = config.get('spark', 'app_name')
        mongo_jar = config.get('spark', 'mongo_jar')

        self.mongodb = config.get('spark', 'mongodb_url')
        self.fecha = fecha

        conf = SparkConf().setAppName(app_name).setMaster(master_url).set("spark.jars.packages", mongo_jar)

        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(conf=conf).config("spark.mongodb.output.uri", self.mongodb).getOrCreate()

    def run(self):
        d = datetime.datetime.strptime(self.fecha, "%Y-%m-%d")
        anio = d.year
        mes = d.month

        anioAnterior = d.year - 1
        mesAnterior = mes - 1

        if mesAnterior == 0:
            mesAnterior = 12
            anio = anioAnterior

        fechaFinMesAnterior = ConsolidadorVariacion.last_day_of_month(datetime.date(anio, mesAnterior, 1))
        fechaFinAnioAnterior = datetime.date(anioAnterior, 12, 31)
        fechaFinMesAnioAnterior = ConsolidadorVariacion.last_day_of_month(datetime.date(anioAnterior, mes, 1))
        
        self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", self.mongodb + "repositorio.posicion_activas").load() \
            .filter("fecha in ('" + self.fecha + "', '" + str(fechaFinMesAnioAnterior) + "', '" + str(fechaFinMesAnterior) + "', '" + str(fechaFinAnioAnterior) + "')") \
            .createOrReplaceTempView("activas")

        self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", self.mongodb + "repositorio.posicion_pasivas").load() \
            .filter("fecha in ('" + self.fecha + "', '" + str(fechaFinMesAnioAnterior) + "', '" + str(fechaFinMesAnterior) + "', '" + str(fechaFinAnioAnterior) + "')") \
            .createOrReplaceTempView("pasivas")

        # PASIVAS
        actualPasivas = self.spark.sql("SELECT * FROM pasivas WHERE fecha = '" + self.fecha + "'").withColumnRenamed("saldo", "valor_final").withColumn("valor_inicial", F.lit(0))
        finMesAnteriorPasivas = self.spark.sql("SELECT * FROM pasivas WHERE fecha = '" + str(fechaFinMesAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))
        fechaFinAnioAnteriorPasivas = self.spark.sql("SELECT * FROM pasivas WHERE fecha = '" + str(fechaFinAnioAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))
        fechaFinMesAnioAnteriorPasivas = self.spark.sql("SELECT * FROM pasivas WHERE fecha = '" + str(fechaFinMesAnioAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))

        actualPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final") \
            .union(finMesAnteriorPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("MENSUAL")).createOrReplaceTempView("variacionesPasivasMensual")

        actualPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final") \
            .union(fechaFinAnioAnteriorPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("ACUMULADA")).createOrReplaceTempView("variacionesPasivasAcumulada")
        
        actualPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final") \
            .union(fechaFinMesAnioAnteriorPasivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("ANUAL")).createOrReplaceTempView("variacionesPasivasAnual")

        # ACTIVAS
        actualActivas = self.spark.sql("SELECT * FROM activas WHERE fecha = '" + self.fecha + "'").withColumnRenamed("saldo", "valor_final").withColumn("valor_inicial", F.lit(0))
        finMesAnteriorActivas = self.spark.sql("SELECT * FROM activas WHERE fecha = '" + str(fechaFinMesAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))
        fechaFinAnioAnteriorActivas = self.spark.sql("SELECT * FROM activas WHERE fecha = '" + str(fechaFinAnioAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))
        fechaFinMesAnioAnteriorActivas = self.spark.sql("SELECT * FROM activas WHERE fecha = '" + str(fechaFinMesAnioAnterior) + "'").withColumnRenamed("saldo", "valor_inicial").withColumn("valor_final", F.lit(0)).withColumn("fecha", F.lit(self.fecha))

        actualActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final") \
            .union(finMesAnteriorActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("MENSUAL")).createOrReplaceTempView("variacionesActivasMensual")

        actualActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final") \
            .union(fechaFinAnioAnteriorActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("ACUMULADA")).createOrReplaceTempView("variacionesActivasAcumulada")
        
        actualActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final") \
            .union(fechaFinMesAnioAnteriorActivas.select("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera", "valor_inicial", "valor_final")) \
            .groupBy("fecha", "tipo_institucion", "nombre_corto", "nombre_largo", "producto", "segmento", "segmento_cartera") \
            .agg(F.sum("valor_inicial").alias("valor_inicial"), F.sum("valor_final").alias("valor_final")) \
            .withColumn("tipo", F.lit("ANUAL")).createOrReplaceTempView("variacionesActivasAnual")

        self.spark.sql("SELECT * FROM variacionesPasivasMensual UNION ALL SELECT * FROM variacionesPasivasAcumulada UNION ALL SELECT * FROM variacionesPasivasAnual") \
            .write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "repositorio").option("collection", "variacion_pasivas").save()

        self.spark.sql("SELECT * FROM variacionesActivasMensual UNION ALL SELECT * FROM variacionesActivasAcumulada UNION ALL SELECT * FROM variacionesActivasAnual") \
            .write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "repositorio").option("collection", "variacion_activas").save()
    
    def last_day_of_month(any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
        return next_month - datetime.timedelta(days=next_month.day)