-- Configuration : 10 is weekly and 11 is monthly
DECLARE @PeriodoTipoID NUMERIC
SET @PeriodoTipoID = 10

DELETE FROM db_analitycs.dbo.aggw_tipo_cambio
WHERE SemanaFuxionID =
    (SELECT PeriodoID
    FROM db_analitycs.dbo.sop_periodos p
    WHERE p.PeriodoTipoID = @PeriodoTipoID
    AND (p.FechaInicio <= '{{ ds }}') AND (p.FechaFin+1 > '{{ ds }}'))

INSERT INTO db_analitycs.dbo.aggw_tipo_cambio (SemanaFuxionID, CodigoMoneda, ValorPromedio, ValorStd,
            FechaActualizacion)

SELECT slice_p.PeriodoID, CodigoMoneda, AVG(TipoDeCambio) AS ValorPromedio, STDEV(TipoDeCambio) AS ValorStd,
        GETDATE() as FechaActualizacion
FROM db_analitycs.dbo.etl_tipo_cambio tc
INNER JOIN
    (SELECT PeriodoID, FechaInicio, FechaFin
    FROM db_analitycs.dbo.sop_periodos p
    WHERE p.PeriodoTipoID = @PeriodoTipoID
    AND (p.FechaInicio <= '{{ ds }}') AND (p.FechaFin+1 > '{{ ds }}')) slice_p
ON (tc.Fecha >= slice_p.FechaInicio) AND (tc.Fecha < (slice_p.FechaFin) + 1)
GROUP BY slice_p.PeriodoID, CodigoMoneda