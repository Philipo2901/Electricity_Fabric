CREATE TABLE [meta].[DataSource] (
    [DataSourceID]     INT            IDENTITY (1, 1) NOT NULL,
    [Name]             NVARCHAR (50)  NOT NULL,
    [ConnectionString] NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_DataSource] PRIMARY KEY CLUSTERED ([DataSourceID] ASC)
);


GO

