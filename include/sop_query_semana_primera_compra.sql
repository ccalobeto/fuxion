-- Only insert processed orders OrderStatusID IN (7,8,9)
INSERT INTO db_analitycs.dbo.sop_semana_primera_compra (ConsumidorID, SemanaFuxionID, FechaOrden, FechaProcesoETL)
SELECT ConsumidorID, MIN(p.PeriodoID), MIN(FechaOrden), GETDATE() AS FechaProcesoETL
  FROM db_analitycs.dbo.etl_ordenes o
  INNER JOIN db_analitycs.dbo.sop_periodos p ON p.PeriodoTipoID=10
  WHERE o.FechaCreacion >= '{{ ds }}' AND o.FechaCreacion < '{{ next_ds }}'
  AND (p.FechaInicio <= o.FechaOrden) AND (p.FechaFin+1 > o.FechaOrden)
  AND o.OrdenStatusID IN (7,8,9)
  AND o.ConsumidorID NOT IN (SELECT ConsumidorID FROM db_analitycs.dbo.sop_semana_primera_compra)
  GROUP BY ConsumidorID
