CREATE TABLE [meta].[DataProductConfiguration] (
    [DataProductConfigurationID] INT            IDENTITY (1, 1) NOT NULL,
    [DataProductID]              INT            NOT NULL,
    [Notebook]                   NVARCHAR (50)  NOT NULL,
    [Dependencies]               NVARCHAR (250) NULL,
    [TimeoutPerCellInSeconds]    INT            NULL,
    [Retry]                      INT            NULL,
    [RetryIntervalInSeconds]     INT            NULL,
    [IsActive]                   BIT            NOT NULL,
    CONSTRAINT [PK_DataProductConfiguration] PRIMARY KEY CLUSTERED ([DataProductConfigurationID] ASC),
    CONSTRAINT [FK_DataProductConfiguration_DataSource] FOREIGN KEY ([DataProductID]) REFERENCES [meta].[DataProduct] ([DataProductID])
);


GO

