CREATE VIEW [meta].[vwActiveImportConfiguration] AS (SELECT D.Name AS [DataSourceName]
       ,ImportConfigurationID
	   ,SourceSchema
       ,SourceTable
       ,SourceQuery
       ,FileName
       ,IsDeltaLoad
       ,WatermarkColumn
       ,WatermarkColumnType
       ,WatermarkValue
       ,WatermarkValueExpression
       ,IsActive
FROM [meta].[ImportConfiguration] I
LEFT JOIN [meta].[DataSource] D ON D.[DataSourceID] = I.[DataSourceID]
WHERE [IsActive] = 1)