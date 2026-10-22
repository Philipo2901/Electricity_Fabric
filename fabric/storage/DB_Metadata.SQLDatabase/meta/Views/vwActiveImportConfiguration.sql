CREATE VIEW [meta].[vwActiveImportConfiguration] AS
(
    SELECT 
        D.Name AS [DataSourceName],
        I.ImportConfigurationID,
        I.SourceSchema,
        I.SourceTable,
        I.SourceQuery,
        I.FileName,
        I.IsDeltaLoad,
        I.WatermarkColumn,
        I.WatermarkColumnType,
        I.WatermarkValue,
        I.WatermarkValueExpression,
        I.IsActive,
        D.ConnectionString -- Neues Feld hinzugefügt
    FROM 
        [meta].[ImportConfiguration] I
    LEFT JOIN 
        [meta].[DataSource] D 
    ON 
        D.[DataSourceID] = I.[DataSourceID]
    WHERE 
        I.[IsActive] = 1
);

GO

