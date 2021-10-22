-- Configuration : 10 is weekly and 11 is monthly
DECLARE @PeriodoTipoID NUMERIC
SET @PeriodoTipoID = 10

DELETE FROM db_analitycs.dbo.aggw_tipo_cambio
WHERE SemanaFuxionID =
    (SELECT PeriodoID
    FROM db_analitycs.dbo.sop_periodos p
    WHERE p.PeriodoTipoID = @PeriodoTipoID
    AND (p.FechaInicio <= '{{ ds }}') AND ('{{ ds }}' < p.FechaFin + 1 ))

INSERT INTO db_analitycs.dbo.aggw_tipo_cambio (SemanaFuxionID, CodigoMoneda, ValorPromedio, ValorStd,
            FechaProcesoResumen)

SELECT slice_p.PeriodoID, CodigoMoneda, AVG(TipoDeCambio) AS ValorPromedio, ISNULL(STDEV(TipoDeCambio), 0) AS ValorStd,
        GETDATE() as FechaProcesoResumen
FROM db_analitycs.dbo.etl_tipo_cambio tc
INNER JOIN
    (SELECT PeriodoID, FechaInicio, FechaFin
    FROM db_analitycs.dbo.sop_periodos p
    WHERE p.PeriodoTipoID = @PeriodoTipoID
    AND (p.FechaInicio <= '{{ ds }}') AND ('{{ ds }}' < p.FechaFin + 1 )) slice_p
ON (slice_p.FechaInicio <= tc.FechaCreacion) AND (tc.FechaCreacion < (slice_p.FechaFin) + 1)
GROUP BY slice_p.PeriodoID, CodigoMoneda