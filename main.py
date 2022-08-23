from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext


sc_conf = SparkConf()
sc_conf.setAppName('COLOMBIA - FARO BT UNIFICADA')
sc_conf.set('spark.yarn.queue','root.enel.batch.CO')
sc_conf.set('spark.driver.memory', '2g')
sc_conf.set('spark.executor.cores', '4')
sc_conf.set('spark.executor.memory', '8g')
sc_conf.set('spark.ui.enabled','true')
sc_conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

sc = SparkContext(conf=sc_conf)
hc = HiveContext(sc)

#HIVE CONFIG
hc.setConf('spark.sql.parquet.compression.codec', 'snappy')
hc.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')


f = open("creates_tables/bt_co_sap_facturacion.hql")
query =  f.read()
f.close()
df = hc.sql(query)







hc.sql('''
CREATE TABLE IF NOT EXISTS `bi_faro.bt_co_sap_facturacion_unificada`(	
  `agrupacion_cargo` string COMMENT 'Agrupacion de cargos por linea de negocio. - maestro de cargos', 	
  `agrup_fac_aju` string, 	
  `agrup_rec` string, 	
  `clasif_fac_aju` string, 	
  `clasif_rec` string, 	
  `anio` int, 	
  `anomalia_facturacion` string, 	
  `cod_barrio` int, 	
  `barrio` string, 	
  `ciclo` int, 	
  `clase_servicio` string, 	
  `cliente_empresarial` string COMMENT 'Tipo de cliente: 1:Colectiva 2: Hija', 	
  `cod_cargo` string COMMENT 'Codigo cargo de acuerdo a maestro de cargos de SAP FICA', 	
  `cargo_op_ppal` string COMMENT 'Operacion principal asociada al cargo', 	
  `cargo_op_parcial` string COMMENT ' Operación parcial asociada al cargo', 	
  `cod_cargo_isu` string, 	
  `cod_categoria` string, 	
  `cod_nivel_tension` string COMMENT 'Nivel tension del cliente', 	
  `cod_producto` string, 	
  `cod_tipo_documento` string, 	
  `consumo_cargo` decimal(31,14) COMMENT 'Consumo del cargo', 	
  `consumo_activo_cargo` double COMMENT 'Consumo de energia activa del cargo', 	
  `consumo_facturado_kwh_activa` double COMMENT 'Consumo de energia activa total de la factura', 	
  `consumo_facturado_kwh_reactiva` double COMMENT 'Consumo de energia reactiva total de la factura', 	
  `corr_ruta_facturacion` string COMMENT 'Correlativo ruta de facturación', 	
  `corr_ruta_lectura` string COMMENT 'Correlativo ruta de lectura', 	
  `corr_ruta_reparto` string COMMENT 'Correlativo ruta de reparto', 	
  `cod_departamento` int, 	
  `departamento` string, 	
  `descripcion_cargo` string, 	
  `dia` int, 	
  `dias_fact_venc` int, 	
  `fecha_proceso` timestamp COMMENT 'Fecha proceso', 	
  `fecha_proceso_anterior` timestamp, 	
  `fecha_lectura_actual` timestamp COMMENT 'Fecha de la lectura actual facturada', 	
  `fecha_lectura_anterior` timestamp COMMENT 'Fecha de la siguiente fecha de lectura', 	
  `duracion_ciclo_fact` double, 	
  `fecha_documento` timestamp COMMENT 'Fecha emisión documento', 	
  `fecha_segundo_vencimiento` timestamp COMMENT 'Fecha segundo vencimiento', 	
  `fecha_contabilizacion` timestamp COMMENT 'Fecha de contabilización del documento en el sistema contable (FICA)', 	
  `duracion_fact_venc` double, 	
  `empresa_servicio` string COMMENT 'Empresa que brinda el servicio facturado', 	
  `es_cuenta_facturable` int, 	
  `esquema_facturacion` string, 	
  `estado_servicio_electrico` string, 	
  `estrato` string, 	
  `factor` decimal(12,5), 	
  `facturacion_diez_ciclos` string COMMENT 'Marca para facturas en lso primeros 10 ciclos facturados del periodo', 	
  `facturacion_quince_dias` string COMMENT 'Marca facturas que son de los primeros 15 dias del mes: del 1 al 15.', 	
  `fec_corte_facturacion` timestamp, 	
  `fecha_aprob_desde` timestamp, 	
  `fecha_aprob_hasta` timestamp, 	
  `fecha_automortizacion` timestamp, 	
  `fecha_corte` timestamp COMMENT 'Fecha de corte del periodo comercial.', 	
  `fecha_primer_vencimiento` timestamp COMMENT 'Fecha primer vencimiento', 	
  `fecha_proxima_lectura` timestamp, 	
  `flag_aplica` string, 	
  `grupo` int, 	
  `grupo_1_cargo` string COMMENT 'Agrupación por  grupo 1', 	
  `grupo_2_cargo` string COMMENT 'Agrupoación por grupo 2', 	
  `ind_informativo` string, 	
  `lat_cliente` double, 	
  `cod_localidad` int, 	
  `localidad` string, 	
  `long_cliente` double, 	
  `manzana` string, 	
  `marcacion_reparto_especial` string, 	
  `mercado` string, 	
  `mes` int, 	
  `monto_cargo` decimal(13,2) COMMENT 'Monto facturado para el cargo', 	
  `monto_facturado_sin_impuesto` decimal(13,2) COMMENT 'Monto total de factura sin impuestos', 	
  `monto_impuesto` decimal(13,2) COMMENT 'Monto total de factura para impuestos', 	
  `monto_quince_dias` decimal(13,2), 	
  `cod_municipio` int, 	
  `municipio` string, 	
  `naturaleza` string, 	
  `negocio` string, 	
  `nit_operador_aseo` string, 	
  `nombre_operador_aseo` string, 	
  `nro_cuenta` int, 	
  `nro_documento` string, 	
  `nro_id_venta` string, 	
  `nro_servicio` string, 	
  `plan` string COMMENT 'Plan de amortización de un convenio', 	
  `precio_unitario` double COMMENT 'Costo unitario de prestacón del servicio expresado en $/kWh', 	
  `segmento_cliente` string, 	
  `segmento_cliente_nivel_2` string, 	
  `sociedad` string, 	
  `socio_codigo` string, 	
  `sub_clase_servicio` string, 	
  `sucursal` int, 	
  `tarifa` string, 	
  `tipo` string, 	
  `tipo_cargo` string, 	
  `tipo_cargo_legales` string COMMENT 'Agrupacion legales', 	
  `tipo_cargo_mso` string COMMENT 'Agrupación de MSO', 	
  `tipo_cliente_empresarial` string, 	
  `tipo_direccion_reparto` string, 	
  `tipo_documento` string, 	
  `tipo_lectura` string COMMENT '01 – Real, 02 - Servicio Directo, 03 – Promediada', 	
  `desc_tipo_lectura` string COMMENT 'Descripción código tipo lectura', 	
  `tipo_liquidacion` string COMMENT 'Indica si el es liquidación mensual o bimensual.', 	
  `tipo_reparto` string, 	
  `tipo_servicio` string, 	
  `total_documento` decimal(13,2), 	
  `ultima_fecha_facturacion` timestamp, 	
  `vence_mes` string COMMENT 'Indicador de si la factura vence en el mismo mes de emisión.', 	
  `zona` int, 	
  `id_cnr` string, 	
  `tarifa_fac` string COMMENT 'Plan contratado por el cliente con la compañía. Actualmente no aplica según, se debe dejar como valor fijo N/A.', 	
  `fecha_dato` timestamp COMMENT 'Es la máxima fecha del campo _fecha_aprob_desde_', 	
  `fecha_insercion_faro` timestamp COMMENT 'Fecha de inserción del registro a la BT transaccional', 	
  `classe_cargo` string, 	
  `cons_activo_leido` string, 	
  `correlativo_facturacion` bigint, 	
  `tipo_tarifa_cargo` string, 	
  `fecha_facturacion_cargo` timestamp, 	
  `identificador_facturas_abiertas` string, 	
  `input_date` timestamp, 	
  `nivel_agrupacion_cargo` string, 	
  `identificador_anio` string, 	
  `num_da_planta_cargo` string, 	
  `numero_cliente_cargo` string, 	
  `setor_cargo` string, 	
  `subclasse_cargo` string, 	
  `tipo_facturacion_cargo` string, 	
  `tipo_documento_cargo` string, 	
  `valor_encargos_aberto_cargo` string, 	
  `descuento_cargo` double, 	
  `costo_unitario_cargo` double, 	
  `factor_cont` double COMMENT 'Porcentaje de contribucion o subsidio de acuerdo al cargo asociado', 	
  `carga_instalada` string, 	
  `pais_odata` string, 	
  `fecha_vig_tarifa` timestamp, 	
  `alt_cliente` double, 	
  `direccion` string, 	
  `tipo_tarifa` string, 	
  `cod_sub_clase_servicio` string, 	
  `costo_generacion` double, 	
  `costo_comercial` double, 	
  `costo_restriccion` double, 	
  `indice_perdidas` double, 	
  `costo_distribucion` double, 	
  `costo_transmision` double, 	
  `otros_costos` double, 	
  `zzcua` double, 	
  `linea_negocio` string COMMENT 'Linea asociada a la deuda', 	
  `fecha_carga_eco` timestamp, 	
  `fecha_carga_s3` timestamp, 	
  `circuito_trafo` string, 	
  `fabricante_medidor` string, 	
  `marca_medidor` string, 	
  `nro_medidor` string, 	
  `nombre_comercial` string, 	
  `propiedad_transf` string, 	
  `tipo_activo` string, 	
  `cod_transformador` string, 	
  `nv_tension_ref` string COMMENT 'Nivel de tension del transformador', 	
  `tipo_red` string, 	
  `consumo_activo_peajes` decimal(20,2), 	
  `consumo_reactivo_peajes` decimal(20,2), 	
  `valor_activo_peajes` decimal(20,4), 	
  `valor_reactivo_peajes` decimal(20,4), 	
  `direccion_frontera_comercial` string, 	
  `cta_padre` string, 	
  `ver_nie_distribucion` bigint, 	
  `mercado_impresion` string, 	
  `fin_periodo_calculo` timestamp, 	
  `inicio_periodo_calculo` timestamp, 	
  `setor` string, 	
  `nombre_cargo` string, 	
  `n_servicio_s3` string, 	
  `nro_familia` int COMMENT 'Numero de Familias facturadas', 	
  `grupo_calidad_ref` string COMMENT 'Grupo calidad asociado al transformador', 	
  `fecha_reg_contable` timestamp COMMENT 'Fecha de registro contable', 	
  `consumo_reactivo_medido` double)	
PARTITIONED BY ( 	
  `periodo` int)	
ROW FORMAT SERDE 	
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 	
STORED AS INPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 	
OUTPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'	
LOCATION	
  'hdfs://nameservice1/data/user/hive/warehouse/bi_faro.db/bt_co_sap_facturacion'	
TBLPROPERTIES (	
  'spark.sql.create.version'='2.4.0-cdh6.3.4', 	
  'spark.sql.sources.schema.numPartCols'='1', 	
  'spark.sql.sources.schema.numParts'='4', 	
  'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"agrupacion_cargo","type":"string","nullable":true,"metadata":{"comment":"Agrupacion de cargos por linea de negocio. - maestro de cargos"}},{"name":"agrup_fac_aju","type":"string","nullable":true,"metadata":{}},{"name":"agrup_rec","type":"string","nullable":true,"metadata":{}},{"name":"clasif_fac_aju","type":"string","nullable":true,"metadata":{}},{"name":"clasif_rec","type":"string","nullable":true,"metadata":{}},{"name":"anio","type":"integer","nullable":true,"metadata":{}},{"name":"anomalia_facturacion","type":"string","nullable":true,"metadata":{}},{"name":"cod_barrio","type":"integer","nullable":true,"metadata":{}},{"name":"barrio","type":"string","nullable":true,"metadata":{}},{"name":"ciclo","type":"integer","nullable":true,"metadata":{}},{"name":"clase_servicio","type":"string","nullable":true,"metadata":{}},{"name":"cliente_empresarial","type":"string","nullable":true,"metadata":{"comment":"Tipo de cliente: 1:Colectiva 2: Hija"}},{"name":"cod_cargo","type":"string","nullable":true,"metadata":{"comment":"Codigo cargo de acuerdo a maestro de cargos de SAP FICA"}},{"name":"cargo_op_ppal","type":"string","nullable":true,"metadata":{"comment":"Operacion principal asociada al cargo"}},{"name":"cargo_op_parcial","type":"string","nullable":true,"metadata":{"comment":" Operación parcial asociada al cargo"}},{"name":"cod_cargo_isu","type":"string","nullable":true,"metadata":{}},{"name":"cod_categoria","type":"string","nullable":true,"metadata":{}},{"name":"cod_nivel_tension","type":"string","nullable":true,"metadata":{"comment":"Nivel tension del cliente"}},{"name":"cod_producto","type":"string","nullable":true,"metadata":{}},{"name":"cod_tipo_documento","type":"string","nullable":true,"metadata":{}},{"name":"consumo_cargo","type":"decimal(31,14)","nullable":true,"metadata":{"comment":"Consumo del cargo"}},{"name":"consumo_activo_cargo","type":"double","nullable":true,"metadata":{"comment":"Consumo de energia activa del cargo"}},{"name":"consumo_facturado_kwh_activa","type":"double","nullable":true,"metadata":{"comment":"Consumo de energia activa total de la factura"}},{"name":"consumo_facturado_kwh_reactiva","type":"double","nullable":true,"metadata":{"comment":"Consumo de energia reactiva total de la factura"}},{"name":"corr_ruta_facturacion","type":"string","nullable":true,"metadata":{"comment":"Correlativo ruta de facturación"}},{"name":"corr_ruta_lectura","type":"string","nullable":true,"metadata":{"comment":"Correlativo ruta de lectura"}},{"name":"corr_ruta_reparto","type":"string","nullable":true,"metadata":{"comment":"Correlativo ruta de reparto"}},{"name":"cod_departamento","type":"integer","nullable":true,"metadata":{}},{"name":"departamento","type":"string","nullable":true,"metadata":{}},{"name":"descripcion_cargo","type":"string","nullable":true,"metadata":{}},{"name":"dia","type":"integer","nullable":true,"metadata":{}},{"name":"dias_fact_venc","type":"integer","nullable":true,"metadata":{}},{"name":"fecha_proceso","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha proceso"}},{"name":"fecha_proceso_anterior","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_lectura_actual","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de la lectura actual facturada"}},{"name":"fecha_lectura_anterior","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de la siguiente fecha de lectura"}},{"name":"duracion_ciclo_fact","type":"double","nullable":true,"metadata":{}},{"name":"fecha_documento","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha emisión documento"}},{"name":"fecha_segundo_vencimiento","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha segundo vencimiento"}},{"name":"fecha_contabilizacion","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de contabilización del documento en el sistema contable (FICA)"}},{"name":"duracion_fact_venc","type":"double","nullable":true,"metadata":{}},{"name":"empres', 	
  'spark.sql.sources.schema.part.1'='a_servicio","type":"string","nullable":true,"metadata":{"comment":"Empresa que brinda el servicio facturado"}},{"name":"es_cuenta_facturable","type":"integer","nullable":true,"metadata":{}},{"name":"esquema_facturacion","type":"string","nullable":true,"metadata":{}},{"name":"estado_servicio_electrico","type":"string","nullable":true,"metadata":{}},{"name":"estrato","type":"string","nullable":true,"metadata":{}},{"name":"factor","type":"decimal(12,5)","nullable":true,"metadata":{}},{"name":"facturacion_diez_ciclos","type":"string","nullable":true,"metadata":{"comment":"Marca para facturas en lso primeros 10 ciclos facturados del periodo"}},{"name":"facturacion_quince_dias","type":"string","nullable":true,"metadata":{"comment":"Marca facturas que son de los primeros 15 dias del mes: del 1 al 15."}},{"name":"fec_corte_facturacion","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_aprob_desde","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_aprob_hasta","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_automortizacion","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_corte","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de corte del periodo comercial."}},{"name":"fecha_primer_vencimiento","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha primer vencimiento"}},{"name":"fecha_proxima_lectura","type":"timestamp","nullable":true,"metadata":{}},{"name":"flag_aplica","type":"string","nullable":true,"metadata":{}},{"name":"grupo","type":"integer","nullable":true,"metadata":{}},{"name":"grupo_1_cargo","type":"string","nullable":true,"metadata":{"comment":"Agrupación por  grupo 1"}},{"name":"grupo_2_cargo","type":"string","nullable":true,"metadata":{"comment":"Agrupoación por grupo 2"}},{"name":"ind_informativo","type":"string","nullable":true,"metadata":{}},{"name":"lat_cliente","type":"double","nullable":true,"metadata":{}},{"name":"cod_localidad","type":"integer","nullable":true,"metadata":{}},{"name":"localidad","type":"string","nullable":true,"metadata":{}},{"name":"long_cliente","type":"double","nullable":true,"metadata":{}},{"name":"manzana","type":"string","nullable":true,"metadata":{}},{"name":"marcacion_reparto_especial","type":"string","nullable":true,"metadata":{}},{"name":"mercado","type":"string","nullable":true,"metadata":{}},{"name":"mes","type":"integer","nullable":true,"metadata":{}},{"name":"monto_cargo","type":"decimal(13,2)","nullable":true,"metadata":{"comment":"Monto facturado para el cargo"}},{"name":"monto_facturado_sin_impuesto","type":"decimal(13,2)","nullable":true,"metadata":{"comment":"Monto total de factura sin impuestos"}},{"name":"monto_impuesto","type":"decimal(13,2)","nullable":true,"metadata":{"comment":"Monto total de factura para impuestos"}},{"name":"monto_quince_dias","type":"decimal(13,2)","nullable":true,"metadata":{}},{"name":"cod_municipio","type":"integer","nullable":true,"metadata":{}},{"name":"municipio","type":"string","nullable":true,"metadata":{}},{"name":"naturaleza","type":"string","nullable":true,"metadata":{}},{"name":"negocio","type":"string","nullable":true,"metadata":{}},{"name":"nit_operador_aseo","type":"string","nullable":true,"metadata":{}},{"name":"nombre_operador_aseo","type":"string","nullable":true,"metadata":{}},{"name":"nro_cuenta","type":"integer","nullable":true,"metadata":{}},{"name":"nro_documento","type":"string","nullable":true,"metadata":{}},{"name":"nro_id_venta","type":"string","nullable":true,"metadata":{}},{"name":"nro_servicio","type":"string","nullable":true,"metadata":{}},{"name":"plan","type":"string","nullable":true,"metadata":{"comment":"Plan de amortización de un convenio"}},{"name":"precio_unitario","type":"double","nullable":true,"metadata":{"comment":"Costo unitario de prestacón del servicio expresado en $/kWh"}},{"name":"segmento_cliente","type":"string","nullable":true,"metadata":{}},{"name":"segmento_cliente_nivel_2","type":"string","nullable":true,"metadata', 	
  'spark.sql.sources.schema.part.2'='":{}},{"name":"sociedad","type":"string","nullable":true,"metadata":{}},{"name":"socio_codigo","type":"string","nullable":true,"metadata":{}},{"name":"sub_clase_servicio","type":"string","nullable":true,"metadata":{}},{"name":"sucursal","type":"integer","nullable":true,"metadata":{}},{"name":"tarifa","type":"string","nullable":true,"metadata":{}},{"name":"tipo","type":"string","nullable":true,"metadata":{}},{"name":"tipo_cargo","type":"string","nullable":true,"metadata":{}},{"name":"tipo_cargo_legales","type":"string","nullable":true,"metadata":{"comment":"Agrupacion legales"}},{"name":"tipo_cargo_mso","type":"string","nullable":true,"metadata":{"comment":"Agrupación de MSO"}},{"name":"tipo_cliente_empresarial","type":"string","nullable":true,"metadata":{}},{"name":"tipo_direccion_reparto","type":"string","nullable":true,"metadata":{}},{"name":"tipo_documento","type":"string","nullable":true,"metadata":{}},{"name":"tipo_lectura","type":"string","nullable":true,"metadata":{"comment":"01 – Real, 02 - Servicio Directo, 03 – Promediada"}},{"name":"desc_tipo_lectura","type":"string","nullable":true,"metadata":{"comment":"Descripción código tipo lectura"}},{"name":"tipo_liquidacion","type":"string","nullable":true,"metadata":{"comment":"Indica si el es liquidación mensual o bimensual."}},{"name":"tipo_reparto","type":"string","nullable":true,"metadata":{}},{"name":"tipo_servicio","type":"string","nullable":true,"metadata":{}},{"name":"total_documento","type":"decimal(13,2)","nullable":true,"metadata":{}},{"name":"ultima_fecha_facturacion","type":"timestamp","nullable":true,"metadata":{}},{"name":"vence_mes","type":"string","nullable":true,"metadata":{"comment":"Indicador de si la factura vence en el mismo mes de emisión."}},{"name":"zona","type":"integer","nullable":true,"metadata":{}},{"name":"id_cnr","type":"string","nullable":true,"metadata":{}},{"name":"tarifa_fac","type":"string","nullable":true,"metadata":{"comment":"Plan contratado por el cliente con la compañía. Actualmente no aplica según, se debe dejar como valor fijo N/A."}},{"name":"fecha_dato","type":"timestamp","nullable":true,"metadata":{"comment":"Es la máxima fecha del campo _fecha_aprob_desde_"}},{"name":"fecha_insercion_faro","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de inserción del registro a la BT transaccional"}},{"name":"classe_cargo","type":"string","nullable":true,"metadata":{}},{"name":"cons_activo_leido","type":"string","nullable":true,"metadata":{}},{"name":"correlativo_facturacion","type":"long","nullable":true,"metadata":{}},{"name":"tipo_tarifa_cargo","type":"string","nullable":true,"metadata":{}},{"name":"fecha_facturacion_cargo","type":"timestamp","nullable":true,"metadata":{}},{"name":"identificador_facturas_abiertas","type":"string","nullable":true,"metadata":{}},{"name":"input_date","type":"timestamp","nullable":true,"metadata":{}},{"name":"nivel_agrupacion_cargo","type":"string","nullable":true,"metadata":{}},{"name":"identificador_anio","type":"string","nullable":true,"metadata":{}},{"name":"num_da_planta_cargo","type":"string","nullable":true,"metadata":{}},{"name":"numero_cliente_cargo","type":"string","nullable":true,"metadata":{}},{"name":"setor_cargo","type":"string","nullable":true,"metadata":{}},{"name":"subclasse_cargo","type":"string","nullable":true,"metadata":{}},{"name":"tipo_facturacion_cargo","type":"string","nullable":true,"metadata":{}},{"name":"tipo_documento_cargo","type":"string","nullable":true,"metadata":{}},{"name":"valor_encargos_aberto_cargo","type":"string","nullable":true,"metadata":{}},{"name":"descuento_cargo","type":"double","nullable":true,"metadata":{}},{"name":"costo_unitario_cargo","type":"double","nullable":true,"metadata":{}},{"name":"factor_cont","type":"double","nullable":true,"metadata":{"comment":"Porcentaje de contribucion o subsidio de acuerdo al cargo asociado"}},{"name":"carga_instalada","type":"string","nullable":true,"metadata":{}},{"name":"pais_odata","type":"string","nullable"', 	
  'spark.sql.sources.schema.part.3'=':true,"metadata":{}},{"name":"fecha_vig_tarifa","type":"timestamp","nullable":true,"metadata":{}},{"name":"alt_cliente","type":"double","nullable":true,"metadata":{}},{"name":"direccion","type":"string","nullable":true,"metadata":{}},{"name":"tipo_tarifa","type":"string","nullable":true,"metadata":{}},{"name":"cod_sub_clase_servicio","type":"string","nullable":true,"metadata":{}},{"name":"costo_generacion","type":"double","nullable":true,"metadata":{}},{"name":"costo_comercial","type":"double","nullable":true,"metadata":{}},{"name":"costo_restriccion","type":"double","nullable":true,"metadata":{}},{"name":"indice_perdidas","type":"double","nullable":true,"metadata":{}},{"name":"costo_distribucion","type":"double","nullable":true,"metadata":{}},{"name":"costo_transmision","type":"double","nullable":true,"metadata":{}},{"name":"otros_costos","type":"double","nullable":true,"metadata":{}},{"name":"zzcua","type":"double","nullable":true,"metadata":{}},{"name":"linea_negocio","type":"string","nullable":true,"metadata":{"comment":"Linea asociada a la deuda"}},{"name":"fecha_carga_eco","type":"timestamp","nullable":true,"metadata":{}},{"name":"fecha_carga_s3","type":"timestamp","nullable":true,"metadata":{}},{"name":"circuito_trafo","type":"string","nullable":true,"metadata":{}},{"name":"fabricante_medidor","type":"string","nullable":true,"metadata":{}},{"name":"marca_medidor","type":"string","nullable":true,"metadata":{}},{"name":"nro_medidor","type":"string","nullable":true,"metadata":{}},{"name":"nombre_comercial","type":"string","nullable":true,"metadata":{}},{"name":"propiedad_transf","type":"string","nullable":true,"metadata":{}},{"name":"tipo_activo","type":"string","nullable":true,"metadata":{}},{"name":"cod_transformador","type":"string","nullable":true,"metadata":{}},{"name":"nv_tension_ref","type":"string","nullable":true,"metadata":{"comment":"Nivel de tension del transformador"}},{"name":"tipo_red","type":"string","nullable":true,"metadata":{}},{"name":"consumo_activo_peajes","type":"decimal(20,2)","nullable":true,"metadata":{}},{"name":"consumo_reactivo_peajes","type":"decimal(20,2)","nullable":true,"metadata":{}},{"name":"valor_activo_peajes","type":"decimal(20,4)","nullable":true,"metadata":{}},{"name":"valor_reactivo_peajes","type":"decimal(20,4)","nullable":true,"metadata":{}},{"name":"direccion_frontera_comercial","type":"string","nullable":true,"metadata":{}},{"name":"cta_padre","type":"string","nullable":true,"metadata":{}},{"name":"ver_nie_distribucion","type":"long","nullable":true,"metadata":{}},{"name":"mercado_impresion","type":"string","nullable":true,"metadata":{}},{"name":"fin_periodo_calculo","type":"timestamp","nullable":true,"metadata":{}},{"name":"inicio_periodo_calculo","type":"timestamp","nullable":true,"metadata":{}},{"name":"setor","type":"string","nullable":true,"metadata":{}},{"name":"nombre_cargo","type":"string","nullable":true,"metadata":{}},{"name":"n_servicio_s3","type":"string","nullable":true,"metadata":{}},{"name":"nro_familia","type":"integer","nullable":true,"metadata":{"comment":"Numero de Familias facturadas"}},{"name":"grupo_calidad_ref","type":"string","nullable":true,"metadata":{"comment":"Grupo calidad asociado al transformador"}},{"name":"fecha_reg_contable","type":"timestamp","nullable":true,"metadata":{"comment":"Fecha de registro contable"}},{"name":"consumo_reactivo_medido","type":"double","nullable":true,"metadata":{}},{"name":"periodo","type":"integer","nullable":true,"metadata":{}}]}', 	
  'spark.sql.sources.schema.partCol.0'='periodo', 	
  'transient_lastDdlTime'='1659984635')	




''')
