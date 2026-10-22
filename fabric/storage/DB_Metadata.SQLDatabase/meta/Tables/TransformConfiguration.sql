CREATE TABLE [meta].[TransformConfiguration] (
    [TransformConfigurationID] INT            IDENTITY (1, 1) NOT NULL,
    [DataSourceID]             INT            NOT NULL,
    [Entity]                   NVARCHAR (128) NOT NULL,
    [IdentifierColumns]        NVARCHAR (128) NOT NULL,
    [IsActive]                 BIT            NOT NULL,
    CONSTRAINT [PK_TransformConfiguration] PRIMARY KEY CLUSTERED ([TransformConfigurationID] ASC),
    CONSTRAINT [FK_TransformConfiguration_DataSource] FOREIGN KEY ([DataSourceID]) REFERENCES [meta].[DataSource] ([DataSourceID])
);


GO

