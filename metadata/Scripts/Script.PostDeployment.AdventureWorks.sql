DECLARE @AdventureWorksDataSourceID INT = 1
DECLARE @AdventureWorksDataSourceName NVARCHAR(20) = 'AdventureWorks';

BEGIN -- DataSource
	SET IDENTITY_INSERT [meta].[DataSource] ON

	IF NOT EXISTS (SELECT 1 FROM [meta].[DataSource] WHERE [Name] = @AdventureWorksDataSourceName)
	BEGIN
		INSERT INTO [meta].[DataSource] ([DataSourceID], [Name], [ConnectionStringSecretName], [Prefix]) 
		VALUES (@AdventureWorksDataSourceID, @AdventureWorksDataSourceName, 'N/A', 'AdventureWorks_');
	END

	SET IDENTITY_INSERT [meta].[DataSource] OFF
END

BEGIN -- ImportConfiguration
	MERGE INTO [meta].[ImportConfiguration] AS Target
	USING (VALUES
		(@AdventureWorksDataSourceID, 'SalesLT', 'Address', 'SELECT * FROM SalesLT.Address', 'SalesLT.Address', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'Customer', 'SELECT * FROM SalesLT.Customer', 'SalesLT.Customer', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'CustomerAddress', 'SELECT * FROM SalesLT.CustomerAddress', 'SalesLT.CustomerAddress', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'Product', 'SELECT * FROM SalesLT.Product', 'SalesLT.Product', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'ProductCategory', 'SELECT * FROM SalesLT.ProductCategory', 'SalesLT.ProductCategory', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'ProductDescription', 'SELECT * FROM SalesLT.ProductDescription', 'SalesLT.ProductDescription', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'ProductModel', 'SELECT * FROM SalesLT.ProductModel', 'SalesLT.ProductModel', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'ProductModelProductDescription', 'SELECT * FROM SalesLT.ProductModelProductDescription', 'SalesLT.ProductModelProductDescription', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'SalesOrderDetail', 'SELECT * FROM SalesLT.SalesOrderDetail', 'SalesLT.SalesOrderDetail', 0, NULL, NULL, NULL, NULL, 1),
		(@AdventureWorksDataSourceID, 'SalesLT', 'SalesOrderHeader', 'SELECT * FROM SalesLT.SalesOrderHeader', 'SalesLT.SalesOrderHeader', 0, NULL, NULL, NULL, NULL, 1)
	)
	AS Source ([DataSourceID],[SourceSchema],[SourceTable],[SourceQuery],[FileName],[IsDeltaLoad],[WatermarkColumn],[WatermarkColumnType],[WatermarkValue],[WatermarkValueExpression],[IsActive])
	ON Target.[DataSourceID] = Source.[DataSourceID]
	AND Target.[SourceSchema] = Source.[SourceSchema]
	AND Target.[SourceTable] = Source.[SourceTable]
	-- update matched rows
	WHEN MATCHED AND
		ISNULL(SOURCE.[SourceQuery], '') <> ISNULL(TARGET.[SourceQuery], '')
		OR ISNULL(SOURCE.[FileName], '') <> ISNULL(TARGET.[FileName], '')
		OR ISNULL(SOURCE.[IsDeltaLoad], 0) <> ISNULL(TARGET.[IsDeltaLoad], 0)
		OR ISNULL(SOURCE.[WatermarkColumn], '') <> ISNULL(TARGET.[WatermarkColumn], '')
		OR ISNULL(SOURCE.[WatermarkColumnType], '') <> ISNULL(TARGET.[WatermarkColumnType], '')
		OR ISNULL(SOURCE.[WatermarkValueExpression], '') <> ISNULL(TARGET.[WatermarkValueExpression], '')
		OR ISNULL(SOURCE.[IsActive], 0) <> ISNULL(TARGET.[IsActive], 0)
	THEN
	UPDATE SET [SourceQuery] = Source.[SourceQuery],
		[FileName] = SOURCE.[FileName],
		[IsDeltaLoad] = SOURCE.[IsDeltaLoad],
		[WatermarkColumn] = SOURCE.[WatermarkColumn],
		[WatermarkColumnType] = SOURCE.[WatermarkColumnType],
		[WatermarkValueExpression] = SOURCE.[WatermarkValueExpression],
		[IsActive] = SOURCE.[IsActive]
	-- insert new rows
	WHEN NOT MATCHED BY TARGET THEN
	INSERT ([DataSourceID],[SourceSchema],[SourceTable],[SourceQuery],[FileName],[IsDeltaLoad],[WatermarkColumn],[WatermarkColumnType],[WatermarkValue],[WatermarkValueExpression],[IsActive])
	VALUES ([DataSourceID],[SourceSchema],[SourceTable],[SourceQuery],[FileName],[IsDeltaLoad],[WatermarkColumn],[WatermarkColumnType],[WatermarkValue],[WatermarkValueExpression],[IsActive])
	-- delete rows that are in the target but not the source
	WHEN NOT MATCHED BY SOURCE AND Target.[DataSourceID] = @AdventureWorksDataSourceID THEN
	DELETE;
END

BEGIN -- TransformConfiguration
	MERGE INTO [meta].[TransformConfiguration] AS Target
	USING (VALUES
		(@AdventureWorksDataSourceID, 'SalesLTAddress', 'AddressID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTCustomer', 'CustomerID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTCustomerAddress', 'CustomerID,AddressID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTProductCategory', 'ProductCategoryID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTProductDescription', 'ProductDescriptionID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTProductModel', 'ProductModelID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTProductModelProductDescription', 'ProductModelID,ProductDescriptionID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTSalesOrderDetail', 'SalesOrderDetailID', 1),
		(@AdventureWorksDataSourceID, 'SalesLTSalesOrderHeader', 'SalesOrderID', 1)
	)
	AS Source ([DataSourceID],[Entity],[IdentifierColumns],[IsActive])
	ON Target.[DataSourceID] = Source.[DataSourceID]
	AND Target.[Entity] = Source.[Entity]
	-- update matched rows
	WHEN MATCHED AND
		ISNULL(SOURCE.[IdentifierColumns], '') <> ISNULL(TARGET.[IdentifierColumns], '')
		OR ISNULL(SOURCE.[IsActive], 0) <> ISNULL(TARGET.[IsActive], 0)
	THEN
	UPDATE SET [IdentifierColumns] = Source.[IdentifierColumns],
		[IsActive] = SOURCE.[IsActive]
	-- insert new rows
	WHEN NOT MATCHED BY TARGET THEN
	INSERT ([DataSourceID],[Entity],[IdentifierColumns],[IsActive])
	VALUES ([DataSourceID],[Entity],[IdentifierColumns],[IsActive])
	-- delete rows that are in the target but not the source
	WHEN NOT MATCHED BY SOURCE AND Target.[DataSourceID] = @AdventureWorksDataSourceID THEN
	DELETE;
END