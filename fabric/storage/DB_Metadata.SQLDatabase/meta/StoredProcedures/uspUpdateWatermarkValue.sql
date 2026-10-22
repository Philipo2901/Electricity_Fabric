
CREATE PROCEDURE [meta].[uspUpdateWatermarkValue]
	@ImportConfigurationID int,
	@WatermarkValue nvarchar(50)
AS

BEGIN

UPDATE [meta].[ImportConfiguration]
SET [WatermarkValue] = @WatermarkValue 
WHERE [ImportConfigurationID] = @ImportConfigurationID;

SELECT 1
END

GO

