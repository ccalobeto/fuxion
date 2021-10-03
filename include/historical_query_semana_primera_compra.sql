-- '2021-08-05' just to take a photo and to test the customers that entered after this date
INSERT INTO db_analitycs.dbo.sop_semana_primera_compra (ConsumidorID, SemanaFuxionID, FechaOrden, FechaActualizacion)
SELECT v.CustomerID, CAST(MAX(Volume12) AS INT), MIN(o_first_row.FirstOrderDate), GETDATE()
    FROM FuxionReporting.dbo.PeriodVolumes v
    INNER JOIN (SELECT CustomerID, MIN(OrderDate) as FirstOrderDate
                    FROM FuxionReporting.dbo.Orders
                    WHERE OrderStatusID IN (7,8,9)
                    GROUP BY CustomerID) o_first_row
        ON v.CustomerID = o_first_row.CustomerID
    WHERE PeriodTypeID=10
    AND Volume12!=0
    AND ModifiedDate< '2021-08-05'
    GROUP BY v.CustomerID
