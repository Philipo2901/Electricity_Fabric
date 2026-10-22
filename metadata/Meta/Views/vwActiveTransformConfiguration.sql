CREATE VIEW [meta].[vwActiveTransformConfiguration] AS (SELECT D.Name AS [DataSourceName]
       ,TransformConfigurationID
       ,Entity
       ,IdentifierColumns
       ,isActive
FROM [meta].[TransformConfiguration] I
LEFT JOIN [meta].[DataSource] D ON D.[DataSourceID] = I.[DataSourceID]
WHERE [IsActive] = 1)