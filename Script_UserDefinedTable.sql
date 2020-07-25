USE [BSC]
GO

/****** Object:  UserDefinedTableType [dbo].[ActiveLedgersType]    Script Date: 7/26/2020 1:38:25 AM ******/
CREATE TYPE [dbo].[ActiveLedgersType] AS TABLE(
	[RowNO] [int] NULL,
	[SystemId] [int] NULL,
	[GeneralId] [int] NULL,
	[LedgersList] [nvarchar](4000) NULL
)
GO


