CREATE TABLE [meta].[DataProduct] (
    [DataProductID] INT           IDENTITY (1, 1) NOT NULL,
    [Name]          NVARCHAR (50) NOT NULL,
    CONSTRAINT [PK_DataProduct] PRIMARY KEY CLUSTERED ([DataProductID] ASC)
);


GO

