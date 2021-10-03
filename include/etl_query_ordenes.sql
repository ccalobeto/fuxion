-- Use templating to extract data by day with CreatedDate
SELECT CustomerID, OrderID, CAST(SIGN(BusinessVolumeTotal) AS INT) AS FlagOrden, ReturnOrderID, CreatedDate, OrderDate,
       ShippedDate, ot.OrderTypeDescription, os.OrderStatusID, Country, o.State, City, Address2, Total, SubTotal,
       DiscountPercent, TaxTotal, ShippingTotal, CurrencyCode, BusinessVolumeTotal, CommissionableVolumeTotal,
       IsCommissionable, GETDATE() AS FechaActualizacion
FROM  FuxionReporting.dbo.Orders o
INNER JOIN FuxionReporting.dbo.OrderTypes ot ON o.OrderTypeID=ot.OrderTypeID
INNER JOIN FuxionReporting.dbo.OrderStatuses os ON o.OrderStatusID=os.OrderStatusID
WHERE CreatedDate >= '{{ ds }}'
AND CreatedDate < '{{ next_ds }}'

