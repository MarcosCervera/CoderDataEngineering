DROP TABLE IF EXISTS marcoscervera_coderhouse.calendario;

-- Crear la tabla de calendario
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