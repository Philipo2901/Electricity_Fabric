CREATE TABLE [meta].[ImportConfiguration] (
    [ImportConfigurationID]  INT             IDENTITY (1, 1) NOT NULL,
    [DataSourceID]           INT             NOT NULL,
    [SourceSchema]           NVARCHAR (128)  NULL,
    [SourceTable]            NVARCHAR (128)  NULL,
    [SourceQuery]            NVARCHAR (MAX)  NULL,
    [FileName]               NVARCHAR (100)  NOT NULL,
    [IsDeltaLoad]            BIT             NULL,
    [WatermarkColumn]        NVARCHAR (128)  NULL,
    [WatermarkColumnType]    NVARCHAR (50)   NULL,
    [WatermarkValue]         NVARCHAR (50)   NULL,
    [WatermarkValueExpression]          NVARCHAR (MAX)   NULL,
    [IsActive]               BIT             DEFAULT (1) NOT NULL,
    CONSTRAINT [PK_ImportConfiguration] PRIMARY KEY CLUSTERED ([ImportConfigurationID] ASC),
    CONSTRAINT [FK_ImportConfiguration_DataSource] FOREIGN KEY ([DataSourceID]) REFERENCES [meta].[DataSource] ([DataSourceID])
);