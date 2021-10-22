SELECT CustomerID, FirstName, LastName, CustomerTypeID, cs.CustomerStatusDescription, Email,
        MobilePhone, MainAddress1, MainAddress2, MainCity, MainState, MainCountry, Gender, EnrollerID,
        BirthDate, CreatedDate, pt.PayableTypeDescription, GETDATE() as FechaProcesoETL
FROM FuxionReporting.dbo.Customers c
INNER JOIN FuxionReporting.dbo.CustomerStatuses cs ON c.CustomerStatusID = cs.CustomerStatusID
INNER JOIN FuxionReporting.dbo.PayableTypes pt ON c.PayableTypeID = pt.PayableTypeID
WHERE CreatedDate >= '{{ ds }}'
AND CreatedDate < '{{ next_ds }}'