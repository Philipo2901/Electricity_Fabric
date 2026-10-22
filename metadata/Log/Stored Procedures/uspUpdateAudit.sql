CREATE PROCEDURE [log].[uspUpdateAudit] @AuditID BIGINT, @EndTime [DATETIME2] (0), @Inserts [BIGINT] = NULL, @Updates [BIGINT] = NULL, @Deletes [BIGINT] = NULL, @ErrorCode [NVARCHAR] (500) = NULL, @ErrorMessage 
	[NVARCHAR] (4000) = NULL, @FailureType [NVARCHAR] (500) = NULL
AS
SET NOCOUNT ON
BEGIN
	DECLARE @HasFailed BIT = CASE WHEN @ErrorMessage IS NULL THEN 0 ELSE 1 END
    DECLARE @UpdatedOn DATETIME = GETDATE();
	BEGIN
		UPDATE logParent
		SET logParent.[EndTime] = CASE WHEN @EndTime IS NULL THEN NULL ELSE @EndTime END, logParent.[DurationInSeconds] = CASE WHEN @EndTime IS NULL THEN NULL ELSE DATEDIFF(SS, [StartTime], @EndTime) END, logParent.[Inserts] = 
			CASE WHEN @Inserts = 0 THEN (
							SELECT ISNULL(SUM(Inserts), 0)
							FROM [log].[Audit] logChild
							WHERE logChild.ParentAuditID = logParent.AuditID
							) ELSE @Inserts END, logParent.[Updates] = CASE WHEN @Updates = 0 THEN (
							SELECT ISNULL(SUM(Updates), 0)
							FROM [log].[Audit] logChild
							WHERE logChild.ParentAuditID = logParent.AuditID
							) ELSE @Updates END, logParent.[Deletes] = @Deletes, logParent.[HasFailed] = @HasFailed, logParent.[ErrorCode] = @ErrorCode, logParent.[ErrorMessage] = @ErrorMessage, logParent.
			[FailureType] = @FailureType,
            [UpdatedOn] = @UpdatedOn
		FROM [log].[Audit] logParent
		WHERE logParent.[AuditID] = @AuditID;
	END
	SELECT 1
END;
GO