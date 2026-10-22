CREATE PROCEDURE [log].[uspInsertAudit] @ParentAuditID [BIGINT], @RunId [NVARCHAR] (50), @ParentRunId [NVARCHAR] (50) = NULL, @ItemName [NVARCHAR] (500), @ItemId [NVARCHAR] (1000), @ComponentName [NVARCHAR] (500
	) = NULL, @StartParameter [NVARCHAR] (500) = NULL, @SourceObject [NVARCHAR] (100), @Source [NVARCHAR] (500) = NULL, @Sink [NVARCHAR] (500) = NULL, @StartTime [DATETIME2] (0), @SourceType [NVARCHAR] (500) = NULL,
    @SinkType [NVARCHAR] (500) = NULL, @WorkspaceId [NVARCHAR] (500) = NULL, @WorkspaceName [NVARCHAR] (500) = NULL, @MasterRunId [NVARCHAR] (500) = NULL, @ItemType [NVARCHAR] (500) = NULL
AS
SET NOCOUNT ON;
BEGIN
	DECLARE @AuditID BIGINT;
	DECLARE @CreatedBy VARCHAR(100) = SUSER_SNAME();
	DECLARE @CreatedOn DATETIME = GETDATE();
	INSERT INTO [log].[Audit] (
		[ParentAuditID], [RunId], [ParentRunId], [ItemName], [ItemId], [ComponentName], [StartParameter], [SourceObject], [Source], [Sink], [SourceType], [SinkType], [StartTime], [EndTime], 
		[DurationInSeconds], [Inserts], [Updates], [Deletes], [HasFailed], [ErrorCode], [ErrorMessage], [FailureType], [CreatedOn], [UpdatedOn], [WorkspaceId], [WorkspaceName], [MasterRunId], [ItemType], [ItemURL], [RunURL]
		)
	VALUES (
		@ParentAuditID, @RunId, @ParentRunId, @ItemName, @ItemId, @ComponentName, @StartParameter, @SourceObject, @Source, @Sink, @SourceType, @SinkType, @StartTime, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, 
		@CreatedOn, NULL, @WorkspaceId, @WorkspaceName, @MasterRunId, @ItemType, ('https://app.powerbi.com/groups/' + @WorkspaceId + '/pipelines/' + @ItemId),
         ('https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces/' + @WorkspaceId + '/pipelines/' + @ItemName + '/' + @RunId)
		)
	SET @AuditID = SCOPE_IDENTITY();
	SELECT @AuditID AS AuditID;
END;
GO