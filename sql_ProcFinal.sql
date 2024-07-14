-- Verifico como se cargó la tabla del etl
select *
from stage_bcra_hash

-- Paso de stage a prod
CALL MoveDataToProd();

-- Verifico como se cargó la tabla de prod
select *
from bcra