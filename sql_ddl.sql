-- Crear la tabla de calendario

DROP TABLE IF EXISTS marcoscervera_coderhouse.calendario;
CREATE TABLE calendario (
    fecha DATE PRIMARY KEY,
    anio INT,
    mes INT,
    dia INT,
    nombre_mes VARCHAR(20),
    nombre_dia VARCHAR(20),
    trimestre INT,
    es_dia_habil BOOLEAN,
    ultimo_dia_mes DATE
);


-- Crear tabla stage (etl api bcra)

DROP TABLE IF EXISTS marcoscervera_coderhouse.stage_bcra_hash;
CREATE TABLE stage_bcra_hash(
	fecha Date,
	base VARCHAR(50),
	cajas_ahorro VARCHAR(50),
	cer VARCHAR(50),
	circulacion_monetaria VARCHAR(50),
	cuentas_corrientes VARCHAR(50),
	depositos VARCHAR(50),
	inflacion_interanual_oficial VARCHAR(50),
	inflacion_mensual_oficial VARCHAR(50),
	plazo_fijo VARCHAR(50),
	reservas VARCHAR(50),
	usd VARCHAR(50),
	usd_of VARCHAR(50),
	usd_of_minorista VARCHAR(50),
	uva VARCHAR(50) ,
	depositos_hash VARCHAR(50)
);


-- Crea tabla prod (de stage bcra)

DROP TABLE IF EXISTS marcoscervera_coderhouse.bcra;
CREATE TABLE bcra(
	fecha Date PRIMARY KEY,
	base INT,
	cajas_ahorro INT,
	cer FLOAT,
	circulacion_monetaria INT,
	cuentas_corrientes varchar(50),
	depositos varchar(50),
	inflacion_interanual_oficial FLOAT,
	inflacion_mensual_oficial FLOAT,
	plazo_fijo INT,
	reservas INT,
	usd FLOAT,
	usd_of FLOAT,
	usd_of_minorista FLOAT,
	uva FLOAT
);


-- Creación de procedimiento para información de tabla stage a producción 
	
CREATE OR REPLACE PROCEDURE MoveDataToProd () 
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM bcra ;
	INSERT INTO bcra (
			fecha,
			base,
			cajas_ahorro,
			cer,
			circulacion_monetaria,
			cuentas_corrientes,
			depositos,
			inflacion_interanual_oficial,
			inflacion_mensual_oficial,
			plazo_fijo,
			reservas,
			usd,
			usd_of,
			usd_of_minorista,
			uva
		)
	SELECT 
		fecha, 
		case when REGEXP_REPLACE(base, '\.0$', '')  ~ '^[0-9]+$' then CAST(REGEXP_REPLACE(base, '\.0$', '') AS INT) else null end as base ,
		case when REGEXP_REPLACE(cajas_ahorro, '\.0$', '')  ~ '^[0-9]+$' then CAST(REGEXP_REPLACE(cajas_ahorro, '\.0$', '') AS INT) else null end as cajas_ahorro,
		case when REGEXP_REPLACE(cer, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(cer AS FLOAT) else null end as cer,
		case when REGEXP_REPLACE(circulacion_monetaria, '\.0$', '')  ~ '^[0-9]+$' then CAST(REGEXP_REPLACE(circulacion_monetaria, '\.0$', '') AS INT) else null end as circulacion_monetaria, 
		cuentas_corrientes_hash as cuentas_corrientes, 
		depositos_hash as depositos, 
		case when REGEXP_REPLACE(inflacion_interanual_oficial, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(inflacion_interanual_oficial AS FLOAT) else null end as inflacion_interanual_oficial,
		case when REGEXP_REPLACE(inflacion_mensual_oficial, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(inflacion_mensual_oficial AS FLOAT) else null end as inflacion_mensual_oficial,
		case when REGEXP_REPLACE(plazo_fijo, '\.0$', '')  ~ '^[0-9]+$' then CAST(REGEXP_REPLACE(plazo_fijo, '\.0$', '') AS INT) else null end as plazo_fijo, 
		case when REGEXP_REPLACE(reservas, '\.0$', '')  ~ '^[0-9]+$' then CAST(REGEXP_REPLACE(reservas, '\.0$', '') AS INT) else null end as reservas, 
		case when REGEXP_REPLACE(usd, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(usd AS FLOAT) else null end  as usd ,
		case when REGEXP_REPLACE(usd_of, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(usd_of AS FLOAT) else null end  as usd_of ,
		case when REGEXP_REPLACE(usd_of_minorista, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(usd_of_minorista AS FLOAT) else null end  as usd_of_minorista,
		case when REGEXP_REPLACE(uva, '\.0$', '')  ~ '^[-+]?[0-9]*\.?[0-9]+$' then CAST(uva AS FLOAT) else null end  as uva
	FROM stage_bcra_hash;
END;
$$;

