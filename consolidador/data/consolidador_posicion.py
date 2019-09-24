# coding=utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from configparser import ConfigParser # se debe cambiar de configparse a ConfigParser en produccion
import os


class ConsolidadorPosicion:
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
        self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
            "uri", self.mongodb + "administracion_sig.producto_cuenta").load().createOrReplaceTempView("productoCuentas")

        self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
            "uri", self.mongodb + "repositorio.balance").load().filter("fecha = '" + self.fecha + "'").createOrReplaceTempView("balance")

        self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option(
            "uri", self.mongodb + "administracion_sig.institucion_financiera").load().createOrReplaceTempView("instituciones")

        self.spark.sql("SELECT b.fecha, i.tipo as tipo_institucion, i.nombre_corto, i.nombre_largo, pc.producto, pc.tipo, pc.segmento, sum(b.saldo) as saldo \
            FROM balance b INNER JOIN productoCuentas pc ON b.cuenta = pc.Cuenta  \
            LEFT JOIN instituciones i on i.codigo = b.institucion \
            GROUP BY b.fecha, i.tipo, i.nombre_corto, i.nombre_largo, pc.producto, pc.tipo, pc.segmento").createOrReplaceTempView("productos")

        pasivas = self.spark.sql("SELECT fecha, tipo_institucion, nombre_corto, nombre_largo, producto, sum(saldo) as saldo FROM productos \
            WHERE producto IN ('Ahorros', 'Monetarios', 'Plazo', 'Otros Depositos') \
            GROUP BY fecha, tipo_institucion, nombre_corto, nombre_largo, producto")

        activas = self.spark.sql("SELECT fecha, tipo_institucion, nombre_corto, nombre_largo, producto, segmento, tipo as segmento_cartera, sum(saldo) as saldo FROM productos \
            WHERE producto IN ('Comercial', 'Consumo', 'Microcredito', 'Vivienda') \
            GROUP BY fecha, tipo_institucion, nombre_corto, nombre_largo, producto, segmento, tipo")

        pasivas.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "repositorio").option("collection", "posicion_pasivas").save()
        activas.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "repositorio").option("collection", "posicion_activas").save()