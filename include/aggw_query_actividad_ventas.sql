DECLARE @start_date DATETIME
DECLARE @end_date DATETIME
DECLARE @PeriodoTipoID NUMERIC
SET @start_date = '2021-01-05'
SET @end_date = '2021-09-20'
SET @PeriodoTipoID = 10

SELECT CodigoPais, slice_p.PeriodoID as SemanaFuxionID, o.ConsumidorID, SUM(OrdenFlag) as CantidadOrdenes,
CAST(SUM(PrecioTotal / atc.ValorPromedio) AS DECIMAL(18,3)) AS VentasProductosUSD, SUM(VolumenTotal) as VolumenPersonal
FROM db_analitycs.dbo.etl_ordenes o
INNER JOIN
        (SELECT PeriodoID, FechaInicio, FechaFIN
        FROM db_analitycs.dbo.sop_periodos p
        WHERE p.PeriodoTipoID = @PeriodoTipoID
        AND (@start_date <= p.FechaInicio) AND (p.FechaFin < @end_date + 1)) slice_p
    ON (slice_p.FechaInicio <= o.FechaOrden) AND (o.FechaOrden < slice_p.FechaFin + 1)
INNER JOIN db_analitycs.dbo.aggw_tipo_cambio atc ON slice_p.PeriodoID=atc.SemanaFuxionID AND
            o.CodigoMoneda=atc.CodigoMoneda
WHERE o.OrdenStatusID IN (7, 8, 9)
AND OrdenFlag != 0
GROUP BY CodigoPais, slice_p.PeriodoID, o.ConsumidorID
ORDER BY CodigoPais, SemanaFuxionID, o.ConsumidorID ASC