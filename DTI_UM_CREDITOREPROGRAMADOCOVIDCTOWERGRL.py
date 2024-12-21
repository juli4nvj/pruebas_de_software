# Databricks notebook source
# coding: utf-8
# **********************************************************************************************************************
# || PROYECTO       : Credito Reprogramado - Control Tower
# || NOMBRE         : DTI_UM_CREDITOREPROGRAMADOCOVIDCTOWERGRL.py
# || TABLA DESTINO  : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowernvocred
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowernvocredcast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowerskipals
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowerskipalscast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowerskipvplu
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowerskipvplucast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowercongals
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowercongalscast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowercongvplu
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowercongvplucast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowerals
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctoweralscast
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowervplu
# ||                : bcp_ddv_rbmctower_datadeliveryctower.dti_um_creditoreprogramadocovidctowervplucast
# || TABLA FUENTE   : BCP_DDV_PYCF_CUBOWU.F_REPORTEGESTION
# ||                : BCP_DDV_RBMBCANEG_INFCONSOLIDADOCLI.MM_PARAMETROGENERAL
# ||                :
# ||                :
# ||                :
# ||                :
# ||                :
# ||                :
# || OBJETIVO       : INSERTAR DATOS A TABLAS DTI
# || TIPO           : PYSPARK
# || REPROCESABLE   : SI - RERUN
# || OBSERVACION    : NA
# || SCHEDULER      : NA
# || JOB            : @P4LKLC4
# || VERSION   DESARROLLADOR        PROVEEDOR      PO                  FECHA             DESCRIPCION
# || -------------------------------------------------------------------------------------------------------------------
# || 1.0.0     JULIAN VALERA        BCP            STEFANNY SEDANO     2024-12-07        INSERTAR DATOS A TABLAS DTI
# **********************************************************************************************************************

# COMMAND ----------

# MAGIC %md
# MAGIC @section Import

# COMMAND ----------

from datetime import datetime, timedelta
from functools import reduce
from dateutil import rrule
from dateutil.relativedelta import relativedelta
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, year, month, date_format, substring, sum, trim, when, first, concat, \
    coalesce, avg, count, row_number, desc, to_date, add_months, date_add
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, DateType, IntegerType
from typing import Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC @section Proceso

# COMMAND ----------


class CreditoReprogramado:
    def __init__(self, prms_spark):
        """
        Logica de proceso para DTI Producto Activo
        """
        self.last_day_12 = None
        self.first_day_12 = None
        self.last_day_6 = None
        self.first_day_6 = None
        self.last_day = None
        self.first_day = None
        self.periodos_12 = None
        self.periodos_6 = None
        self.codmes_inicio_12 = None
        self.codmes_inicio_6 = None
        self.codmes_fin = None
        self.codmes_proceso = None
        self.spark = SparkSession.builder.getOrCreate()
        self.cons_num_resta_meses_0 = 0
        self.cons_num_resta_meses_12 = 11
        self.cons_num_resta_meses_6 = 5
        self.cons_left = "left"
        self.cons_inner = "inner"

        self.prm_spark_fecharutina = prms_spark["prm_spark_fecharutina"]

        self.prm_spark_esquema_ddv_infconsolidadocli = prms_spark["prm_spark_esquema_ddv_infconsolidadocli"]
        self.prm_spark_esquema_ddv_pycf_cubowu = prms_spark["prm_spark_esquema_ddv_pycf_cubowu"]
        self.prm_spark_esquema_udv_int = prms_spark["prm_spark_esquema_udv_int"]
        self.prm_spark_esquema_rdv_de_lkde = prms_spark["prm_spark_esquema_rdv_de_lkde"]
        self.prm_spark_esquema_ddv_rbmctower_portafolioregulatorio = prms_spark["prm_spark_esquema_ddv_rbmctower_portafolioregulatorio"]

        self.prm_spark_tabla_mm_parametrogeneral = prms_spark["prm_spark_tabla_mm_parametrogeneral"]
        self.prm_spark_tabla_f_reportegestion = prms_spark["prm_spark_tabla_f_reportegestion"]
        self.prm_spark_tabla_lfi_codproductogestion = prms_spark["prm_spark_tabla_lfi_codproductogestion"]
        self.prm_spark_tabla_h_saldocontable = prms_spark["prm_spark_tabla_h_saldocontable"]
        self.prm_spark_tabla_m_cliente = prms_spark["prm_spark_tabla_m_cliente"]
        self.prm_spark_tabla_de_creditobeneficiootorgadoctower = prms_spark["prm_spark_tabla_de_creditobeneficiootorgadoctower"]
        self.prm_spark_tabla_h_cuentatarjetacredito = prms_spark["prm_spark_tabla_h_cuentatarjetacredito"]
        self.prm_spark_tabla_um_creditoreprogramadocovidctower = prms_spark["prm_spark_tabla_um_creditoreprogramadocovidctower"]
        self.prm_spark_tabla_h_cuentafinanciera = prms_spark["prm_spark_tabla_h_cuentafinanciera"]
        self.prm_spark_tabla_h_saldocuentacreditopersonal = prms_spark["prm_spark_tabla_h_saldocuentacreditopersonal"]
        self.prm_spark_tabla_h_saldocuentatarjetacredito = prms_spark["prm_spark_tabla_h_saldocuentatarjetacredito"]
        self.prm_spark_tabla_de_creditoreduccioncuotactower = prms_spark["prm_spark_tabla_de_creditoreduccioncuotactower"]
        self.prm_spark_tabla_de_proximovencimientocuotactower = prms_spark["prm_spark_tabla_de_proximovencimientocuotactower"]
        self.prm_spark_tabla_m_cuentafinanciera = prms_spark["prm_spark_tabla_m_cuentafinanciera"]

        self.cons_mm_parametrogeneral = f"{self.prm_spark_esquema_ddv_infconsolidadocli}/{self.prm_spark_tabla_mm_parametrogeneral}"
        self.cons_f_reportegestion = f"{self.prm_spark_esquema_ddv_pycf_cubowu}/{self.prm_spark_tabla_f_reportegestion}"
        self.cons_lfi_codproductogestion = f"{self.prm_spark_esquema_ddv_pycf_cubowu}/{self.prm_spark_tabla_lfi_codproductogestion}"
        self.cons_h_saldocontable = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_h_saldocontable}"
        self.cons_m_cliente = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_m_cliente}"
        self.cons_de_creditobeneficiootorgadoctower = f"{self.prm_spark_esquema_rdv_de_lkde}/{self.prm_spark_tabla_de_creditobeneficiootorgadoctower}"
        self.cons_h_cuentatarjetacredito = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_h_cuentatarjetacredito}"
        self.cons_um_creditoreprogramadocovidctower = f"{self.prm_spark_esquema_ddv_rbmctower_portafolioregulatorio}/{self.prm_spark_tabla_um_creditoreprogramadocovidctower}"
        self.cons_h_cuentafinanciera = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_h_cuentafinanciera}"
        self.cons_h_saldocuentacreditopersonal = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_h_saldocuentacreditopersonal}"
        self.cons_h_saldocuentatarjetacredito = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_h_saldocuentatarjetacredito}"
        self.cons_de_creditoreduccioncuotactower = f"{self.prm_spark_esquema_rdv_de_lkde}/{self.prm_spark_tabla_de_creditoreduccioncuotactower}"
        self.cons_de_proximovencimientocuotactower = f"{self.prm_spark_esquema_rdv_de_lkde}/{self.prm_spark_tabla_de_proximovencimientocuotactower}"
        self.cons_m_cuentafinanciera = f"{self.prm_spark_esquema_udv_int}/{self.prm_spark_tabla_m_cuentafinanciera}"

    @staticmethod
    def _logger_info(mensaje):
        print("::: " + mensaje)

    @staticmethod
    def read_data_lhcl(spark, schema_tabla) -> DataFrame:
        """
        Lectura de tabla en Catalog
        """
        try:
            # df = spark.read.table(schema_tabla)
            df = spark.read.parquet(schema_tabla)
            return df
        except Exception as e:
            raise Exception(f"Error en read_data_lhcl:\n{e}")

    @staticmethod
    def write_data_lhcl(df, w_format, w_mode, schema_table):
        """
        Escritura de tabla en lhcl
        """
        try:
            (df
             .write
             .format(w_format)
             .mode(w_mode)
             .option("partitionOverwriteMode","dynamic")
             .insertInto(schema_table))
        except Exception as e:
            raise Exception(f"Error en write_data_lhcl:\n{e}")

    @property
    def col_current_datetime(self):
        """
        Genera la fecha actual
        """
        return datetime.now()

    @staticmethod
    def get_schema(schema) -> [list]:
        """
        Genera el esquema de dataframes
        """
        try:
            return [col(c.name).cast(c.dataType).alias(c.name) for c in schema]
        except Exception as e:
            raise Exception(f"Error en get_schema:\n{e}")

    @staticmethod
    def schema_hm_insumopuntuacionprospectoingresobdn():
        """
        Esquema de dataframes hm_insumopuntuacionprospectoingresobdn
        """
        return StructType([
            StructField("codclavepartycli", StringType(), nullable=True),
            StructField("codclavepartydeudorsbs", StringType(), nullable=True),
            StructField("tiproldeudorsbs", StringType(), nullable=True),
            StructField("codempsistemafinanciero", StringType(), nullable=True),
            StructField("codsbs", StringType(), nullable=True),
            StructField("codclaveunicocli", StringType(), nullable=True),
            StructField("codinternocomputacional", StringType(), nullable=True),
            StructField("codsector", StringType(), nullable=True),
            StructField("nbrempsistemafinanciero", StringType(), nullable=True),
            StructField("rcc_mto_deu_vig", DecimalType(23, 6), nullable=True),
            StructField("fecrutina", DateType(), nullable=True),
            StructField("fecactualizacionregistro", TimestampType(), nullable=True),
            StructField("codmes", IntegerType(), nullable=True)
        ])

    @staticmethod
    def get_union_dataframe(*dfs: DataFrame) -> DataFrame:
        """
        Union de los DataFrames
        """
        try:
            if not dfs:
                raise Exception("No DataFrames for union")
            for df in dfs:
                if not isinstance(df, DataFrame):
                    raise Exception("All inputs must be DataFrame")
            union_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs).dropDuplicates()
            return union_df
        except BaseException as e:
            raise Exception(":: Error en get_union_dataframe :: \n:: Error: " + str(e))

    @staticmethod
    def get_periodos_a_procesar(codmes_inicio, codmes_fin):
        """
        Crea arreglo de periodos a procesar
        """
        try:
            array_periodos = []
            periodo_inicio = datetime.strptime(codmes_inicio, "%Y%m")
            periodo_fin = datetime.strptime(codmes_fin, "%Y%m")
            for dt in rrule.rrule(rrule.MONTHLY, dtstart=periodo_inicio, until=periodo_fin):
                periodo_codmes = int(str(dt.date()).replace("-", "")[:6])
                array_periodos.append(periodo_codmes)
            return array_periodos
        except BaseException as e:
            raise Exception(":: Error en get_periodos_a_procesar :: \n:: Error: " + str(e))

    @staticmethod
    def restar_meses(meses, fecha):
        """
        Resta meses a codmes
        """
        try:
            resta_n_meses = int(meses)
            fecha = str(fecha)
            fecha_inicial = datetime.strptime(fecha, "%Y%m")
            resultado_fecha = fecha_inicial - relativedelta(months=resta_n_meses)
            fecha_fin = int(str(resultado_fecha).replace("-", "")[:6])
            return fecha_fin
        except BaseException as e:
            raise Exception(":: Error en restar_meses :: \n:: Error: " + str(e))

    @staticmethod
    def get_days(codmes_inicio, codmes_fin):
        """
        Obtiene el primer día de codmes_inicio y el último día de codmes_fin
        """
        try:
            # Obtener el primer día de codmes_inicio
            codmes_inicio = str(codmes_inicio)
            year_inicio = int(codmes_inicio[:4])
            month_inicio = int(codmes_inicio[4:])
            first_day = datetime(year_inicio, month_inicio, 1).date()

            # Obtener el último día de codmes_fin
            codmes_fin = str(codmes_fin)
            year_fin = int(codmes_fin[:4])
            month_fin = int(codmes_fin[4:])
            first_day_fin = datetime(year_fin, month_fin, 1).date()
            next_month = first_day_fin.replace(day=28) + timedelta(days=4)
            last_day = next_month - timedelta(days=next_month.day)

            return first_day, last_day
        except BaseException as e:
            raise Exception(":: Error en get_days :: \n:: Error: " + str(e))

    @property
    def get_columns_dates(self):
        """
        Retorna columnas date y reject
        """
        dates_list = [lit(self.prm_spark_fecharutina).alias("fecrutina"),
                      lit(self.col_current_datetime).alias("fecactualizacionregistro")]

        tipo_rej = [lit("KEY").alias("tiporeject")]
        return dates_list, tipo_rej

    @property
    def get_mm_parametrogeneral(self) -> DataFrame:
        """
        Obtiene datos de la tabla mm_parametrogeneral
        """
        try:
            filter_conditions = (col("codparametro") == lit('CODMES'))
            mm_parametrogeneral_df = self.read_data_lhcl(self.spark, self.cons_mm_parametrogeneral)
            return mm_parametrogeneral_df.filter(filter_conditions) \
                .select(col("codmesinicio"),
                        col("codmesfin")
                        )
        except BaseException as e:
            raise Exception(":: Error en get_mm_parametrogeneral :: \n" + str(e))

    @property
    def get_parametros(self):
        """
            Crea arreglo de periodos a procesar
        """
        try:
            array_periodos_proceso = []
            periodos = self.get_mm_parametrogeneral.first()
            if periodos:
                codmes_inicio = str(periodos['codmesinicio'])
                codmes_fin = str(periodos['codmesfin'])
                array_periodos_proceso = self.get_periodos_a_procesar(codmes_inicio, codmes_fin)
            return array_periodos_proceso
        except BaseException as e:
            raise Exception(":: Error en get_parametros :: \n" + str(e))

    @property
    def get_h_saldocontable(self) -> DataFrame:
        """
        Obtiene datos de h_saldocontable
        """
        try:
            h_saldocontable_df = self.read_data_lhcl(self.spark, self.cons_h_saldocontable)
            max_fecdia = str(h_saldocontable_df.select(to_date(max(col("fecdia")))).first()[0])
            h_saldocontable_ftr_df = (h_saldocontable_df
                                      .filter(to_date(col("fecdia")) == max_fecdia)
                                      .select(col("codclavepartycli"),
                                              col("tipdeudoracreedor"))
                                      ).dropDuplicates()

            return h_saldocontable_ftr_df
        except BaseException as e:
            raise Exception(f"Error en get_h_saldocontable:\n{e}")

    @property
    def get_maestra_cuentas_f(self) -> DataFrame:
        """
        PASO 1 - Obtiene cuentas maestras
        """
        try:
            f_reportegestion_df = self.read_data_lhcl(self.spark, self.cons_f_reportegestion)
            lfi_codproductogestion_df = self.read_data_lhcl(self.spark, self.cons_lfi_codproductogestion)
            h_saldocontable_df = self.get_h_saldocontable
            m_cliente_df = self.read_data_lhcl(self.spark, self.cons_m_cliente)

            maestra_cuentas_df = (f_reportegestion_df
                                  .filter((date_format(col("fecrutina"), 'yyyyMM') == self.codmes_proceso) &
                                          (col("codfuentedato").isin('VPLU', 'ALS')) &
                                          (substring(col("codctacontablefin"), 1, 2) == '14') &
                                          (col("tipllavemoneda") == 1) &
                                          (col("flgregistrocargocredito") == 'N'))
                                  .alias("rg")
                                  .join(lfi_codproductogestion_df
                                        .filter((col("flgultimoregistro") == 'S') &
                                                (col("desproductogestionnivel18") == 'ACTCO0001 - Colocaciones'))
                                        .alias("cp"),
                                        on=["codproductogestion"],
                                        how=self.cons_left)
                                  .join(h_saldocontable_df
                                        .alias("sc"),
                                        on=["codclavepartycli"],
                                        how=self.cons_left)
                                  .join(m_cliente_df
                                        .filter(col("flgregeliminadofuente") == 'N')
                                        .alias("cl"),
                                        on=["codclavepartycli"],
                                        how=self.cons_left)
                                  .groupBy(col("rg.codclavecta"), col("rg.codmoneda"), col("rg.codclavepartycli"),
                                           col("rg.fecdia"), col("rg.codfuentedato"),
                                           col("rg.codsubsegmentofinancierofin"),
                                           col("sc.tipdeudoracreedor"),
                                           col("cl.codsubsegmento"), col("cl.codinternocomputacional")
                                           )
                                  .agg(max(when(trim(col("rg.codproductofin")) == 'CTEORD', 'CTAACT').otherwise(trim(col("rg.codproductofin")))).alias("codproducto"),
                                       coalesce(sum(col("rg.mtosaldodia")).cast(DecimalType(23, 6)), lit(0)).alias("mtosaldofinmessol"),
                                       sum(col("rg.mtosaldomedio")).alias("mtosaldomedio")
                                       )
                                  )
            return (maestra_cuentas_df
                    .select(col("codclavecta"),
                            col("codclavepartycli"),
                            col("codproducto"))
                    .dropDuplicates())
        except BaseException as e:
            raise Exception(f"Error en get_maestra_cuentas_f:\n{e}")

    @property
    def get_temp_bd_repro_total(self) -> DataFrame:
        """
        PASO 2 -
        """
        try:
            de_creditobeneficiootorgadoctower_df = self.read_data_lhcl(self.spark,
                                                                       self.cons_de_creditobeneficiootorgadoctower)
            h_cuentatarjetacredito_df = self.read_data_lhcl(self.spark, self.cons_h_cuentatarjetacredito)
            codclavecta_1_df = de_creditobeneficiootorgadoctower_df.select(col("codclavecta"))
            codclavecta_2_df = de_creditobeneficiootorgadoctower_df.select(col("codclavectapostbeneficio").alias("codclavecta"))
            codclavecta_df = self.get_union_dataframe(codclavecta_1_df, codclavecta_2_df)
            temp_cuentas_tc_df = (h_cuentatarjetacredito_df.filter(col("fecrutina") == self.last_day).alias("cu")
                                  .join(codclavecta_df.alias("de"), on=["codclavecta"], how=self.cons_inner)
                                  .select("codclavecta")
                                  )
            repro_total_df = (de_creditobeneficiootorgadoctower_df.alias("a")
                              .join(temp_cuentas_tc_df.alias("b"), on=col("a.codclavecta") == col("b.codclavecta"), how=self.cons_left)
                              .join(temp_cuentas_tc_df.alias("c"), on=col("a.codclavectapostbeneficio") == col("c.codclavecta"), how=self.cons_left)
                              .select(col("a.codclavecta"),
                                      col("a.codclavectapostbeneficio"),
                                      col("b.codclavecta").alias("codopecta"),
                                      col("c.codclavecta").alias("codopecta_n"),
                                      col("a.destipaccionamientobeneficio"),
                                      col("a.fecejecucionbeneficio")
                                      )
                              )
            return repro_total_df
        except BaseException as e:
            raise Exception(f"Error en get_temp_bd_repro_total:\n{e}")


    @property
    def get_maestra_reprogramados(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            um_creditoreprogramadocovidctower_df = self.read_data_lhcl(self.spark, self.cons_um_creditoreprogramadocovidctower)
            h_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_h_cuentafinanciera)
            get_maestra_cuentas_f_df = self.get_maestra_cuentas_f

            maestra_reprogramados_df = (um_creditoreprogramadocovidctower_df.alias("a")
                                        .join(get_maestra_cuentas_f_df.alias("b"), on=col("a.codclavectapostbeneficio") == col("b.codclavecta"), how=self.cons_left)
                                        .join(h_cuentafinanciera_df.filter(col("fecrutina") == self.last_day).alias("c"), on=col("b.codclavecta") == col("c.codclavecta"), how=self.cons_left)
                                        .select(col("b.codclavecta"),
                                                col("a.codclavectapostbeneficio"),
                                                col("c.codmoneda"),
                                                col("b.codproducto")
                                                )
                                        )

            return maestra_reprogramados_df
        except BaseException as e:
            raise Exception(f"Error en get_maestra_reprogramados:\n{e}")

    @property
    def get_maximomora_als_ff(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            h_saldocuentacreditopersonal_df = self.read_data_lhcl(self.spark, self.cons_h_saldocuentacreditopersonal)
            maximomora_als_ff_df = (h_saldocuentacreditopersonal_df
                                    .filter(col("fecdia").between(self.first_day, self.last_day))
                                    .groupBy(date_format(col("fecdia"), 'yyyyMM').alias("CODMES"),
                                             col("codclavecta"))
                                    .agg(max(col("ctddiavcda")).alias("ctddiavcda"))
                                    )

            return maximomora_als_ff_df
        except BaseException as e:
            raise Exception(f"Error en get_maximomora_als_ff:\n{e}")

    @property
    def get_maximomora_vp_f0_t(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            h_saldocuentatarjetacredito_df = self.read_data_lhcl(self.spark, self.cons_h_saldocuentatarjetacredito)
            maximomora_vp_f0_t_df = (h_saldocuentatarjetacredito_df
                                     .filter(col("fecdia").between(self.first_day, self.last_day))
                                     .groupBy(date_format(col("fecdia"), 'yyyyMM').alias("CODMES"),
                                              col("codclavecta"))
                                     .agg(max(col("ctddiamoroso")).alias("ctddiamoroso"))
                                     )

            return maximomora_vp_f0_t_df
        except BaseException as e:
            raise Exception(f"Error en get_maximomora_vp_f0_t:\n{e}")

    @property
    def get_temp_cuotas_amor_limpieza(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            h_saldocuentacreditopersonal_df = self.read_data_lhcl(self.spark, self.cons_h_saldocuentacreditopersonal)
            temp_cuotas_amor_limpieza_df = (h_saldocuentacreditopersonal_df
                                            .filter(col("fecdia").between(self.first_day_6, self.last_day_6))
                                            .groupBy(col("codclavecta"))
                                            .agg(max(when(date_format(col("fecdia"), 'yyyyMM') == self.codmes_inicio_6, col("ctdpagopendiente")).otherwise(lit(0))).alias("cuota_act"),
                                                 max(when(date_format(col("fecdia"), 'yyyyMM') == self.codmes_fin, col("ctdpagopendiente")).otherwise(lit(0))).alias("cuota_ant6")
                                                 )
                                            )

            return temp_cuotas_amor_limpieza_df
        except BaseException as e:
            raise Exception(f"Error en get_temp_cuotas_amor_limpieza:\n{e}")

    @property
    def get_union_data_entrys(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            de_creditobeneficiootorgadoctower_df = self.read_data_lhcl(self.spark, self.cons_de_creditobeneficiootorgadoctower)
            de_creditoreduccioncuotactower_df = self.read_data_lhcl(self.spark, self.cons_de_creditoreduccioncuotactower)
            de_credito_1_df = de_creditobeneficiootorgadoctower_df.filter(~col("destipaccionamientobeneficio").isin("Skip", "Congelamiento")).select(col("codclavecta"))
            de_credito_2_df = de_creditobeneficiootorgadoctower_df.filter(~col("destipaccionamientobeneficio").isin("Skip", "Congelamiento")).select(col("codclavectapostbeneficio").alias("codclavecta"))
            de_credito_3_df = de_creditoreduccioncuotactower_df.select(col("codclavecta"))
            union_data_entrys = self.get_union_dataframe(de_credito_1_df, de_credito_2_df, de_credito_3_df)
            return union_data_entrys
        except BaseException as e:
            raise Exception(f"Error en get_union_data_entrys:\n{e}")

    @property
    def get_limpieza_als_0(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            maestra_reprogramados_df = self.get_maestra_reprogramados
            data_entrys_df = self.get_union_data_entrys
            maestra_cuentas_f_df = self.get_maestra_cuentas_f
            de_proximovencimientocuotactower_df = self.read_data_lhcl(self.spark, self.cons_de_proximovencimientocuotactower)
            h_saldocuentacreditopersonal_df = self.read_data_lhcl(self.spark, self.cons_h_saldocuentacreditopersonal)
            maximomora_als_ff_df = self.get_maximomora_als_ff
            temp_cuotas_amor_limpieza_df = self.get_temp_cuotas_amor_limpieza

            limpieza_als_0_df = (maestra_reprogramados_df.alias("a")
                                 .join(data_entrys_df.alias("b"), on=col("a.codclavecta") == col("b.codclavecta"), how=self.cons_left)
                                 .join(maestra_cuentas_f_df.alias("c"), on=col("a.codclavecta") == col("c.codclavecta"), how=self.cons_left)
                                 .join(de_proximovencimientocuotactower_df.filter(col("fecvctoprimeracuotapostbeneficio").isNotNull()).alias("d"), on=col("c.codclavecta") == col("d.codclavecta"), how=self.cons_left)
                                 .join(h_saldocuentacreditopersonal_df.alias("e"), on=(col("c.codclavecta") == col("e.codclavecta")) & (date_format(col("d.fecvctoprimeracuotapostbeneficio"), 'yyyyMM') == date_format(col("e.fecdia"), 'yyyyMM')), how=self.cons_left)
                                 .join(h_saldocuentacreditopersonal_df.alias("f"), on=(col("c.codclavecta") == col("f.codclavecta")) & (date_format(add_months(col("d.fecvctoprimeracuotapostbeneficio"), 1), 'yyyyMM') == date_format(col("f.fecdia"), 'yyyyMM')), how=self.cons_left)
                                 .join(h_saldocuentacreditopersonal_df.alias("g"), on=(col("c.codclavecta") == col("g.codclavecta")) & (col("g.fecdia") == self.last_day), how=self.cons_left)
                                 .join(maximomora_als_ff_df.alias("h"), on=col("c.codclavecta") == col("h.codclavecta"), how=self.cons_left)
                                 .join(temp_cuotas_amor_limpieza_df.alias("i"), on=col("a.codclavecta") == col("i.codclavecta"), how=self.cons_left)
                                 .select(
                                         col("a.codclavecta"),
                                         col("a.codmoneda"),
                                         col("h.codmes"),
                                         when(col("b.codclavecta").isNotNull(), lit('REDUCC')).otherwise(lit('NO REDUCC')).alias("flg_red_cuot"),
                                         col("d.fecvctoprimeracuotapostbeneficio"),
                                         coalesce(col("e.mtosaldocapital"), col("f.mtosaldocapital")).alias("saldo_inicial"),
                                         when((col("h.codmes") >= date_format(col("d.fecvctoprimeracuotapostbeneficio"), "yyyyMM")) & (col("h.ctddiavcda").between(0, 8)), lit(1)).otherwise(lit(0)).alias("flg_pago"),
                                         col("g.mtosaldocapital").alias("saldo_final"),
                                         col("h.ctddiavcda"),
                                         (col("i.cuota_ant6") - col("i.cuota_act")).alias("cuotas_pagadas")
                                         )
                                 .dropDuplicates()
                                 )

            return limpieza_als_0_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_als_0:\n{e}")

    @property
    def get_limpieza_als_nr_f(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            limpieza_als_0_df = self.get_limpieza_als_0
            limpieza_als_nr_f_df = (
                limpieza_als_0_df.filter(
                    (col("saldo_final") > 0) &
                    (col("flg_red_cuot") == "NO REDUCC") &
                    (col("codmes").between(202403, 202407)) & # ACTUALIZAR VALOR
                    (col("cuotas_pagadas") >= 6)
                )
                .groupBy("codclavecta")
                .agg(
                    avg(
                        when(col("codmoneda") == "1001", col("saldo_final") * 4).otherwise(col("saldo_final"))
                    ).alias("saldo_final"),
                    sum(col("flg_pago")).alias("sum_flg_pago")
                )
                .filter(col("sum_flg_pago") == 6)
                .select(lit(202407).alias("codmes"), # ACTUALIZAR VALOR
                        col("codclavecta"),
                        col("saldo_final")
                        )
            )
            return limpieza_als_nr_f_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_als_nr_f:\n{e}")

    @property
    def get_limpieza_als_redu_f(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            limpieza_als_0_df = self.get_limpieza_als_0
            limpieza_als_redu_f_df = (
                limpieza_als_0_df.filter(
                    (col("saldo_inicial") > 0) &
                    (col("flg_red_cuot") == "REDUCC") &
                    (col("codmes").between(202402, 202406)) & # ACTUALIZAR VALOR
                    (col("saldo_final") / coalesce(col("saldo_inicial"), lit(0)) <= 0.8) &
                    (col("cuotas_pagadas") >= 6)
                )
                .groupBy("codclavecta")
                .agg(
                    avg(
                        when(col("codmoneda") == "1001", col("saldo_final") * 4).otherwise(col("saldo_final"))
                    ).alias("saldo_final"),
                    sum(col("flg_pago")).alias("sum_flg_pago")
                )
                .filter(col("sum_flg_pago") == 6)
                .select(lit(202406).alias("codmes"), # ACTUALIZAR VALOR
                        col("codclavecta"),
                        col("saldo_final")
                        )
            )
            return limpieza_als_redu_f_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_als_redu_f:\n{e}")

    @property
    def get_limpieza_vp_0(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            maestra_reprogramados_df = self.get_maestra_reprogramados
            de_creditoreduccioncuotactower_df = self.read_data_lhcl(self.spark, self.cons_de_creditoreduccioncuotactower)
            de_proximovencimientocuotactower_df = self.read_data_lhcl(self.spark, self.cons_de_proximovencimientocuotactower)
            maximomora_vp_f0_t_df = self.get_maximomora_vp_f0_t

            limpieza_vp_0_df = (maestra_reprogramados_df.alias("a")
                                .join(de_creditoreduccioncuotactower_df.alias("b"), on=col("a.codclavecta") == col("b.codclavecta"), how=self.cons_left)
                                .join(de_proximovencimientocuotactower_df.filter(col("fecvctoprimeracuotapostbeneficio").isNotNull()).alias("c"), on=col("a.codclavecta") == col("c.codclavecta"), how=self.cons_left)
                                .join(maximomora_vp_f0_t_df.filter(col("codmes") == self.codmes_proceso).alias("d"), on=col("a.codclavecta") == col("d.codclavecta"), how=self.cons_left)
                                .select(col("a.codclavecta"),
                                        col("d.codmes"),
                                        when(col("b.codclavecta").isNotNull(), lit('PAGO MINIMO')).otherwise('PAGO FULL').alias("flg_red_cuot"),
                                        col("c.fecvctoprimeracuotapostbeneficio"),
                                        col("d.ctddiamoroso"),
                                        when((col("d.codmes") >= date_format(col("c.fecvctoprimeracuotapostbeneficio"), 'yyyyMM')) & (col("d.ctddiamoroso").between(0, 8)), lit(1)).otherwise(lit(0)).alias("flg_pago")
                                        )
                                )
            return limpieza_vp_0_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_vp_0:\n{e}")

    @property
    def get_limpieza_vp_pagmin_f(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:

            # ACTUALIZAR VALOR

            limpieza_vp_0_df = self.get_limpieza_vp_0
            limpieza_vp_pagmin_f_df = (
                limpieza_vp_0_df.filter(
                    (col("flg_red_cuot") == 'PAGO MINIMO') &
                    (col("codmes").between(202401, 202406))
                )
                .groupBy(col("codclavecta"))
                .agg(sum(col("flg_pago")).alias("sum_flg_pago"))
                .filter(col("sum_flg_pago") == 6)
                .select(
                    lit(202406).alias("codmes"),
                    col("codclavecta")
                )
            )
            return limpieza_vp_pagmin_f_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_vp_pagmin_f:\n{e}")

    @property
    def get_limpieza_vp_pagfull_f(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            # ACTUALIZAR VALOR

            limpieza_vp_0_df = self.get_limpieza_vp_0
            limpieza_vp_pagmin_f_df = (
                limpieza_vp_0_df.filter(
                    (col("flg_red_cuot") == 'PAGO FULL') &
                    (col("codmes").between(202307, 202406))
                )
                .groupBy(col("codclavecta"))
                .agg(sum(col("flg_pago")).alias("sum_flg_pago"))
                .filter(col("sum_flg_pago") == 12)
                .select(
                    lit(202406).alias("codmes"),
                    col("codclavecta")
                )
            )
            return limpieza_vp_pagmin_f_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_vp_pagfull_f:\n{e}")

    @property
    def get_mm_limpieza_reprogramados(self) -> DataFrame:
        """
        PASO 4 -
        """
        try:
            limpieza_als_nr_f_df = self.get_limpieza_als_nr_f
            limpieza_als_redu_f_df = self.get_limpieza_als_redu_f
            limpieza_vp_pagmin_f_df = self.get_limpieza_vp_pagmin_f
            limpieza_vp_pagfull_f_df = self.get_limpieza_vp_pagfull_f

            # ACTUALIZAR VALOR

            limpieza_als_nr_df = (limpieza_als_nr_f_df
                                  .filter(col("codmes") == 202406)
                                  .select(col("codmes"), col("codclavecta")))
            limpieza_als_redu_df = (limpieza_als_redu_f_df
                                    .filter(col("codmes") == 202406)
                                    .select(col("codmes"), col("codclavecta")))
            limpieza_vp_pagmin_df = (limpieza_vp_pagmin_f_df
                                     .filter(col("codmes") == 202406)
                                     .select(col("codmes"), col("codclavecta")))
            limpieza_vp_pagfull_df = (limpieza_vp_pagfull_f_df
                                      .filter(col("codmes") == 202406)
                                      .select(col("codmes"), col("codclavecta")))

            limpieza_vp_pagfull_f_df = self.get_union_dataframe(limpieza_als_nr_df,
                                                                limpieza_als_redu_df,
                                                                limpieza_vp_pagmin_df,
                                                                limpieza_vp_pagfull_df
                                                                )
            return limpieza_vp_pagfull_f_df
        except BaseException as e:
            raise Exception(f"Error en get_mm_limpieza_reprogramados:\n{e}")

    @property
    def get_cte_fam_max_1(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            temp_bd_repro_total_df = self.get_temp_bd_repro_total
            m_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_m_cuentafinanciera)

            temp_bd_repro_1_df = temp_bd_repro_total_df.select(col("codclavecta"),
                                                               col("codopecta"),
                                                               col("fecejecucionbeneficio"))
            temp_bd_repro_2_df = temp_bd_repro_total_df.select(col("codclavectapostbeneficio").alias("codclavecta"),
                                                               col("codopecta_n").alias("codopecta"),
                                                               col("fecejecucionbeneficio"))
            temp_bd_repro_df = self.get_union_dataframe(temp_bd_repro_1_df, temp_bd_repro_2_df)
            bd_repro_df = (temp_bd_repro_df
                           .groupBy(col("codclavecta"), col("codopecta"))
                           .agg(max(col("fecejecucionbeneficio")).alias("fecejecucionbeneficio"))
                           )
            cte_fam_max_1_df = (bd_repro_df.alias("a")
                                .join(m_cuentafinanciera_df
                                      .filter(col("codapp") == 'ALS')
                                      .alias("b"), on=col("a.codclavecta") == col("b.codclavecta"), how=self.cons_left
                                      ).select(col("a.codclavecta"),
                                               col("a.codopecta"),
                                               col("a.fecejecucionbeneficio"),
                                               date_format(col("a.fecejecucionbeneficio"), 'yyyyMM')
                                               .alias("codmes_fam"),
                                               when(col("b.codclavecta").isNotNull(), date_format(
                                                   date_add(col("a.fecejecucionbeneficio"), 6), 'yyyyMM'))
                                               .otherwise(date_format(
                                                   date_add(col("a.fecejecucionbeneficio"), 12), 'yyyyMM'))
                                               .alias("codmes_fam6")
                                               )
                                )
            return cte_fam_max_1_df
        except BaseException as e:
            raise Exception(f"Error en get_cte_fam_max_1:\n{e}")

    @property
    def get_mm_limpieza_repro_con_lag_1(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            mm_limpieza_reprogramados_df = self.get_mm_limpieza_reprogramados
            cte_fam_max_1_df = self.get_cte_fam_max_1
            mm_limpieza_repro_con_lag_1_df = (mm_limpieza_reprogramados_df.alias("a")
                                              .join(cte_fam_max_1_df.alias("b"),
                                                    on=col("a.codclavecta") == col("b.codclavecta"),
                                                    how=self.cons_left
                                                    )
                                              .join(cte_fam_max_1_df.alias("c"),
                                                    on=col("a.codclavecta") == col("c.codopecta"),
                                                    how=self.cons_left
                                                    )
                                              .select(col("a.codmes"),
                                                      col("a.codclavecta"),
                                                      coalesce(col("b.codmes_fam6"),
                                                               col("c.codmes_fam6")).alias("codmes_fam6"),
                                                      when(col("a.codmes") < col("b.codmes_fam6"), lit(1))
                                                      .when(col("a.codmes") < col("c.codmes_fam6"), lit(1))
                                                      .otherwise(lit(0)).alias("flg_cuenta")
                                                      )
                                              )
            return mm_limpieza_repro_con_lag_1_df
        except BaseException as e:
            raise Exception(f"Error en get_mm_limpieza_repro_con_lag_1:\n{e}")

    @property
    def get_mm_tarjetaslimpias_repros(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            mm_limpieza_repro_con_lag_1_df = self.get_mm_limpieza_repro_con_lag_1
            m_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_m_cuentafinanciera)

            limpieza_repro_df = (mm_limpieza_repro_con_lag_1_df.alias("a")
                                 .join(m_cuentafinanciera_df.filter(col("codapp") == 'VPLU').alias("b"),
                                       on=col("a.codclavecta") == col("b.codclavecta"),
                                       how=self.cons_inner)
                                 # .filter(col("flg_cuenta") == lit(0)) # ACTUALIZAR VALOR
                                 .select(col("a.codclavecta"),
                                         col("a.codmes"),
                                         col("a.codmes_fam6"),
                                         col("a.flg_cuenta")
                                         )
                                 )
            mm_tarjetaslimpias_repros_df = (limpieza_repro_df.filter(col("flg_cuenta") == lit(0))
                                            .groupBy(col("codclavecta"),
                                                     col("codmes_fam6"),
                                                     col("flg_cuenta")
                                                     )
                                            .agg(max(col("codmes")).alias("codmes"))
                                            )
            return mm_tarjetaslimpias_repros_df
        except BaseException as e:
            raise Exception(f"Error en get_mm_tarjetaslimpias_repros:\n{e}")

    @property
    def get_cte_fam_max(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            temp_bd_repro_total_df = self.get_temp_bd_repro_total
            mm_tarjetaslimpias_repros_df = self.get_mm_tarjetaslimpias_repros
            m_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_m_cuentafinanciera)

            bd_repro_total_1_df = (temp_bd_repro_total_df.select(col("codclavecta"),
                                                                 col("codopecta"),
                                                                 col("fecejecucionbeneficio")
                                                                 )
                                   )
            bd_repro_total_2_df = (temp_bd_repro_total_df.select(col("codclavectapostbeneficio").alias("codclavecta"),
                                                                 col("codopecta_n").alias("codopecta"),
                                                                 col("fecejecucionbeneficio")
                                                                 )
                                   )
            bd_repro_total_df = self.get_union_dataframe(bd_repro_total_1_df, bd_repro_total_2_df)
            repro_total_df = (bd_repro_total_df.groupBy(col("codclavecta"), col("codopecta"))
                                               .agg(max(col("fecejecucionbeneficio")).alias("fecejecucionbeneficio"))
                              )
            cte_fam_max_df = (repro_total_df.alias("a")
                              .join(mm_tarjetaslimpias_repros_df.alias("b"),
                                    on=col("a.codopecta") == col("b.codclavecta"),
                                    how=self.cons_left)
                              .join(m_cuentafinanciera_df.filter(col("codapp") == lit('ALS')).alias("c"),
                                    on=col("a.codclavecta") == col("c.codclavecta"),
                                    how=self.cons_left)
                              .select(col("a.codclavecta"),
                                      col("a.codopecta"),
                                      col("a.fecejecucionbeneficio"),
                                      date_format(col("a.fecejecucionbeneficio"), 'yyyyMM').alias("codmes_fam"),
                                      when((col("b.codclavecta").isNotNull()) &
                                           (col("a.fecejecucionbeneficio") < lit('2022-10-01')), # ACTUALIZAR VALOR
                                           date_format(date_add(col("a.fecejecucionbeneficio"),  6), 'yyyyMM'))
                                      .when(col("c.codclavecta").isNotNull(),
                                            date_format(date_add(col("a.fecejecucionbeneficio"),  6), 'yyyyMM'))
                                      .otherwise(date_format(date_add(col("a.fecejecucionbeneficio"), 12), 'yyyyMM'))
                                      .alias("codmes_fam6")
                                      )
                              )

            return cte_fam_max_df
        except BaseException as e:
            raise Exception(f"Error en get_cte_fam_max:\n{e}")

    @property
    def get_mm_limpieza_repro_con_lag(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            mm_limpieza_reprogramados_df = self.get_mm_limpieza_reprogramados
            cte_fam_max_df = self.get_cte_fam_max

            mm_limpieza_repro_con_lag_df = (mm_limpieza_reprogramados_df.alias("a")
                                            .join(cte_fam_max_df.alias("b"), on=col("a.codclavecta") == col("b.codclavecta"), how=self.cons_left)
                                            .join(cte_fam_max_df.alias("C"), on=col("a.codclavecta") == col("c.codopecta"), how=self.cons_left)
                                            .select(col("a.codmes"),
                                                    col("a.codclavecta"),
                                                    coalesce(col("b.codmes_fam6"), col("c.codmes_fam6"))
                                                    .alias("codmes_fam6"),
                                                    when(col("a.codmes") < col("b.codmes_fam6"), lit(1))
                                                    .when(col("a.codmes") < col("c.codmes_fam6"), lit(1))
                                                    .otherwise(lit(0)).alias("flg_cuenta")
                                                    )
                                            )
            return mm_limpieza_repro_con_lag_df
        except BaseException as e:
            raise Exception(f"Error en get_mm_limpieza_repro_con_lag:\n{e}")

    @property
    def get_limpieza_vp_sinsaldo_f(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            cte_fam_max_df = self.get_cte_fam_max
            h_saldocuentatarjetacredito_df = self.read_data_lhcl(self.spark, self.cons_h_saldocuentatarjetacredito)
            m_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_m_cuentafinanciera)

            saldocuentatarjetacredito_df = (h_saldocuentatarjetacredito_df
                                            .filter(date_format(col("fecdia"), 'yyyymm') >= 202101) # ACTUALIZAR VALOR por "2021-01-01"
                                            .select(col("codclavecta"),
                                                    col("mtobalanceactual"),
                                                    date_format(col("fecdia"), 'yyyyMM').alias("codmes")
                                                    )
                                            )
            cuentafinanciera_df = (m_cuentafinanciera_df.filter(col("codapp") == lit('VPLU'))
                                                        .select(col("codclavecta"),
                                                                col("codapp")
                                                                )
                                   )
            limpieza_vp_sinsaldo_df = (cte_fam_max_df.alias("a")
                                       .join(saldocuentatarjetacredito_df.alias("b"),
                                             on=col("a.codclavecta") == col("b.codclavecta"),
                                             how=self.cons_left)
                                       .join(cuentafinanciera_df.alias("c"),
                                             on=col("a.codclavecta") == col("c.codclavecta"),
                                             how=self.cons_inner)
                                       .groupBy(col("a.codclavecta"),
                                                col("a.codopecta"),
                                                col("b.codmes"),
                                                col("a.codmes_fam"))
                                       .agg(sum(when(col("b.mtobalanceactual") > 0, lit(1)).otherwise(lit(0)))
                                            .alias("flg_cero"))
                                       )
            limpieza_vp_sinsaldo_f_df = (limpieza_vp_sinsaldo_df.filter((col("flg_cero") == lit(0)) &
                                                                        (col("codmes") >= col("codmes_fam")))
                                                                .select(col("codopecta"))
                                                                .dropDuplicates()
                                         )
            return limpieza_vp_sinsaldo_f_df
        except BaseException as e:
            raise Exception(f"Error en get_limpieza_vp_sinsaldo_f:\n{e}")

    @property
    def get_mm_limpieza_repro_con_lag(self) -> DataFrame:
        """
        PASO 5 -
        """
        try:
            mm_limpieza_repro_con_lag_df = self.get_mm_limpieza_repro_con_lag
            m_cuentafinanciera_df = self.read_data_lhcl(self.spark, self.cons_m_cuentafinanciera)

            cuentafinanciera_df = (m_cuentafinanciera_df.filter(col("codapp") == lit('VPLU'))
                                   .select(col("codclavecta"),
                                           col("codapp")
                                           )
                                   )
            limpieza_vp_df = (mm_limpieza_repro_con_lag_df.alias("a")
                              .join(cuentafinanciera_df.alias("b"),
                                    on=col("a.codclavecta") == col("b.codclavecta"),
                                    how=self.cons_inner)
                              .filter("a.flg_cuenta")
                              .select(col("a.codmes"),
                                      col("a.codclavecta"),
                                      col("a.codmes_fam6"),
                                      col("a.flg_cuenta")
                                      )
                              )



            return mm_limpieza_repro_con_lag_df
        except BaseException as e:
            raise Exception(f"Error en get_mm_limpieza_repro_con_lag:\n{e}")



    def process_data(self) -> None:
        self._logger_info(" >>> Inicia proceso ...")
        try:
            periodos_arr = self.get_parametros
            for periodo in periodos_arr:
                self._logger_info("Periodo en proceso: " + str(periodo))
                self.codmes_proceso = periodo

                self.codmes_fin = self.restar_meses(self.cons_num_resta_meses_0, self.codmes_proceso)

                self.codmes_inicio_6 = self.restar_meses(self.cons_num_resta_meses_6, self.codmes_proceso)
                self.codmes_inicio_12 = self.restar_meses(self.cons_num_resta_meses_12, self.codmes_proceso)

                self.periodos_6 = self.get_periodos_a_procesar(str(self.codmes_inicio_6), str(self.codmes_fin))
                self.periodos_12 = self.get_periodos_a_procesar(str(self.codmes_inicio_12), str(self.codmes_fin))

                self.periodos_6.sort(reverse=True)
                self.periodos_12.sort(reverse=True)

                self.first_day, self.last_day = self.get_days(self.codmes_proceso, self.codmes_proceso)
                self.first_day_6, self.last_day_6 = self.get_days(self.codmes_inicio_6, self.codmes_proceso)
                self.first_day_12, self.last_day_12 = self.get_days(self.codmes_inicio_12, self.codmes_proceso)

                print("codmes_proceso", self.codmes_proceso)
                print("codmes_fin", self.codmes_fin)
                print("codmes_inicio_6", self.codmes_inicio_6)
                print("codmes_inicio_12", self.codmes_inicio_12)
                print("periodos_6", self.periodos_6)
                print("periodos_12", self.periodos_12)
                print("first_day", self.first_day)
                print("last_day", self.last_day)
                print("first_day_6", self.first_day_6)
                print("last_day_6", self.last_day_6)
                print("first_day_12", self.first_day_12)
                print("last_day_12", self.last_day_12)

                df = self.get_limpieza_vp_sinsaldo_f
                df.show()



        except Exception as e:
            self._logger_info(f"\nError en process_data:\n{e}")
        self._logger_info(" >>> Finaliza proceso...")

# COMMAND ----------

# MAGIC %md
# MAGIC main()

# COMMAND ----------


def main():
    try:
        # dbutils.widgets.text("PRM_SPARK_FECHARUTINA", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_UDV_INT", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_DDV_INFCONSOLIDADOCLI", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_DDV_PYCF_CUBOWU", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_RDV_DE_LKDE", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_RDV_DE_LKDE", "")
        # dbutils.widgets.text("PRM_SPARK_ESQUEMA_EDV_SBXM_006", "")

        PRM_SPARK_FECHARUTINA = '2024-12-11'
        PRM_SPARK_ESQUEMA_DDV_INFCONSOLIDADOCLI = 'C:\\Users\\T30403\\Documents\\COTROL_TOWER\\data'
        PRM_SPARK_ESQUEMA_DDV_PYCF_CUBOWU = 'C:\\Users\\T30403\\Documents\\COTROL_TOWER\\data'
        PRM_SPARK_ESQUEMA_UDV_INT = 'C:\\Users\\T30403\\Documents\\COTROL_TOWER\\data'
        PRM_SPARK_ESQUEMA_RDV_DE_LKDE = 'C:\\Users\\T30403\\Documents\\COTROL_TOWER\\data'
        PRM_SPARK_ESQUEMA_DDV_RBMCTOWER_PORTAFOLIOREGULATORIO = 'C:\\Users\\T30403\\Documents\\COTROL_TOWER\\data'

        PRM_SPARK_TABLA_MM_PARAMETROGENERAL = 'mm_parametrogeneral'
        PRM_SPARK_TABLA_F_REPORTEGESTION = 'f_reportegestion'
        PRM_SPARK_TABLA_LFI_CODPRODUCTOGESTION = 'lfi_codproductogestion'
        PRM_SPARK_TABLA_H_SALDOCONTABLE = 'h_saldocontable'
        PRM_SPARK_TABLA_M_CLIENTE = 'm_cliente'
        PRM_SPARK_TABLA_DE_CREDITOBENEFICIOOTORGADOCTOWER = 'de_creditobeneficiootorgadoctower'
        PRM_SPARK_TABLA_H_CUENTATARJETACREDITO = 'h_cuentatarjetacredito'
        PRM_SPARK_TABLA_UM_CREDITOREPROGRAMADOCOVIDCTOWER = 'um_creditoreprogramadocovidctower'
        PRM_SPARK_TABLA_H_CUENTAFINANCIERA = 'h_cuentafinanciera'
        PRM_SPARK_TABLA_H_SALDOCUENTACREDITOPERSONAL = 'h_saldocuentacreditopersonal'
        PRM_SPARK_TABLA_H_SALDOCUENTATARJETACREDITO = 'h_saldocuentatarjetacredito'
        PRM_SPARK_TABLA_DE_CREDITOREDUCCIONCUOTACTOWER = 'de_creditoreduccioncuotactower'
        PRM_SPARK_TABLA_DE_PROXIMOVENCIMIENTOCUOTACTOWER = 'de_proximovencimientocuotactower'
        PRM_SPARK_TABLA_M_CUENTAFINANCIERA = 'm_cuentafinanciera'


        prms_spark = {
            # "prm_spark_fecharutina": dbutils.widgets.get("PRM_SPARK_FECHARUTINA"),
            # "prm_spark_esquema_ddv_seginfobcaneg": dbutils.widgets.get("PRM_SPARK_ESQUEMA_DDV_SEGINFOBCANEG"),
            # "prm_spark_esquema_ddv_matrizvariables": dbutils.widgets.get("PRM_SPARK_ESQUEMA_DDV_MATRIZVARIABLES"),

            "prm_spark_fecharutina": PRM_SPARK_FECHARUTINA,
            "prm_spark_esquema_ddv_infconsolidadocli": PRM_SPARK_ESQUEMA_DDV_INFCONSOLIDADOCLI,
            "prm_spark_esquema_ddv_pycf_cubowu": PRM_SPARK_ESQUEMA_DDV_PYCF_CUBOWU,
            "prm_spark_esquema_udv_int": PRM_SPARK_ESQUEMA_UDV_INT,
            "prm_spark_esquema_rdv_de_lkde": PRM_SPARK_ESQUEMA_RDV_DE_LKDE,
            "prm_spark_esquema_ddv_rbmctower_portafolioregulatorio": PRM_SPARK_ESQUEMA_DDV_RBMCTOWER_PORTAFOLIOREGULATORIO,

            "prm_spark_tabla_mm_parametrogeneral": PRM_SPARK_TABLA_MM_PARAMETROGENERAL,
            "prm_spark_tabla_f_reportegestion": PRM_SPARK_TABLA_F_REPORTEGESTION,
            "prm_spark_tabla_lfi_codproductogestion": PRM_SPARK_TABLA_LFI_CODPRODUCTOGESTION,
            "prm_spark_tabla_h_saldocontable": PRM_SPARK_TABLA_H_SALDOCONTABLE,
            "prm_spark_tabla_m_cliente": PRM_SPARK_TABLA_M_CLIENTE,
            "prm_spark_tabla_de_creditobeneficiootorgadoctower": PRM_SPARK_TABLA_DE_CREDITOBENEFICIOOTORGADOCTOWER,
            "prm_spark_tabla_h_cuentatarjetacredito": PRM_SPARK_TABLA_H_CUENTATARJETACREDITO,
            "prm_spark_tabla_um_creditoreprogramadocovidctower": PRM_SPARK_TABLA_UM_CREDITOREPROGRAMADOCOVIDCTOWER,
            "prm_spark_tabla_h_cuentafinanciera": PRM_SPARK_TABLA_H_CUENTAFINANCIERA,
            "prm_spark_tabla_h_saldocuentacreditopersonal": PRM_SPARK_TABLA_H_SALDOCUENTACREDITOPERSONAL,
            "prm_spark_tabla_h_saldocuentatarjetacredito": PRM_SPARK_TABLA_H_SALDOCUENTATARJETACREDITO,
            "prm_spark_tabla_de_creditoreduccioncuotactower": PRM_SPARK_TABLA_DE_CREDITOREDUCCIONCUOTACTOWER,
            "prm_spark_tabla_de_proximovencimientocuotactower": PRM_SPARK_TABLA_DE_PROXIMOVENCIMIENTOCUOTACTOWER,
            "prm_spark_tabla_m_cuentafinanciera": PRM_SPARK_TABLA_M_CUENTAFINANCIERA



        }

        credito_reprogramado = CreditoReprogramado(prms_spark)
        credito_reprogramado.process_data()
    except Exception as e:
        # dbutils.notebook.exit("Error en main() \n" + str(e))
        print("Error en main() \n" + str(e))

# COMMAND ----------


if __name__ == '__main__':
    main()

# COMMAND ----------

# dbutils.notebook.exit(1)
