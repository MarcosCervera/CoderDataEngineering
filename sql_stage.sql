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


