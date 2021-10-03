INSERT INTO db_analitycs.dbo.etl_ordenes (ConsumidorID, OrdenID, OrdenFlag, OrdenIDRetorno, FechaCreacion, FechaOrden,
            FechaEnvio, OrdenTipo, OrdenStatusID, CodigoPais, Estado, Provincia, Distrito, PrecioTotal,
            PrecioSubTotalIncDescuento, PorcentajeDescuento,
            ImpuestoTotal, CostoEnvioTotal, CodigoMoneda, VolumenTotal, VolumenComisionableTotal, EsComisionable,
            FechaActualizacion)
SELECT CustomerID, OrderID, CAST(SIGN(BusinessVolumeTotal) AS INT) AS FlagOrden, ReturnOrderID, CreatedDate, OrderDate,
       ShippedDate, ot.OrderTypeDescription, os.OrderStatusID, Country, o.State, City, Address2, Total, SubTotal,
       DiscountPercent, TaxTotal, ShippingTotal, CurrencyCode, BusinessVolumeTotal, CommissionableVolumeTotal,
       IsCommissionable, GETDATE() AS FechaActualizacion
FROM  FuxionReporting.dbo.Orders o
INNER JOIN FuxionReporting.dbo.OrderTypes ot ON o.OrderTypeID=ot.OrderTypeID
INNER JOIN FuxionReporting.dbo.OrderStatuses os ON o.OrderStatusID=os.OrderStatusID
WHERE CreatedDate >= '2021-01-01'
AND CreatedDate < '2021-08-31'