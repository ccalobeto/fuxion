INSERT INTO db_analitycs.dbo.etl_consumidores (ConsumidorID, Nombres, Apellidos, TipoConsumidorID, EstadoConsumidor,
            Email, Celular, Direccion, Distrito, Ciudad, Estado, CodigoPais, Sexo, PatrocinadorID, FechaNacimiento,
            FechaCreacion, FormaPago, FechaActualizacion)
SELECT CustomerID, FirstName, LastName, CustomerTypeID, cs.CustomerStatusDescription, Email,
        MobilePhone, MainAddress1, MainAddress2, MainCity, MainState, MainCountry, Gender, EnrollerID,
        BirthDate, CreatedDate, pt.PayableTypeDescription, GETDATE() as FechaActualizacion
FROM FuxionReporting.dbo.Customers c
INNER JOIN FuxionReporting.dbo.CustomerStatuses cs ON c.CustomerStatusID = cs.CustomerStatusID
INNER JOIN FuxionReporting.dbo.PayableTypes pt ON c.PayableTypeID = pt.PayableTypeID
WHERE CreatedDate >= '{{ ds }}'
AND CreatedDate < '{{ next_ds }}'