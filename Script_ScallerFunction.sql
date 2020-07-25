USE [BSC]
GO

/****** Object:  UserDefinedFunction [dbo].[Fix_Letter]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Create function [dbo].[Fix_Letter](@S nvarchar(4000)) returns nvarchar(4000) as 
Begin
	DECLARE @R nvarchar(4000)
	SET @R = REPLACE(REPLACE(@S, NCHAR(1603),NCHAR(1705)),NCHAR(1610),NCHAR(1740))
	RETURN(@R)
End;
GO

/****** Object:  UserDefinedFunction [dbo].[FN_CanDeleteFormula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_CanDeleteFormula]( @FormulaId int ) RETURNS nvarchar(4000) AS
BEGIN

	DECLARE @Ret nvarchar(4000)='',@Code nvarchar(100);
	SELECT @Code = '['+Code+']' From Formulas Where FormulaId=@FormulaId;

	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			SeqNo, Code, '['+Code+']' as Code2, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	),
	CTE_ALL AS
	(
		SELECT 
			SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY dbo.FN_StringToTable_Not_NULL( UsedLedgersAbr2 ,',') A
	),
	CTE_RETUEN AS
	(
	SELECT DISTINCT B.Code2 
	FROM CTE_ALL A
	INNER JOIN  (SELECT SeqNo,Code2, Lst FROM CTE_ALL WHERE Lst = @Code) B	ON B.Lst = A.Code2
	)
	SELECT @Ret= @Ret+ CASE @Ret WHEN '' THEN '' ELSE ',' END+Code2 FROM CTE_RETUEN;

	RETURN @RET 
END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_CheckNewFormula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_CheckNewFormula]( @FormulaId int, @SeqNO int, @Name nvarchar(200), @Code nvarchar(100) ) RETURNS int AS
BEGIN

	Declare @ERR int = 0;

	IF EXISTS (SELECT 1 FROM Formulas WHERE SeqNO = @SeqNO AND FormulaId <> @FormulaId) --SeqNO Error
		SET @ERR = -101
	ELSE
	IF EXISTS (SELECT 1 FROM Formulas WHERE Code = @Code AND FormulaId <> @FormulaId)   -- @Code Error
		SET @ERR = -102
	ELSE
	IF EXISTS (SELECT 1 FROM Formulas WHERE Name = @Name AND FormulaId <> @FormulaId)   -- @Name Erroe
		SET @ERR = -103

	RETURN @ERR;

END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_CheckPrivateFormula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_CheckPrivateFormula]( @FormulaId int ) RETURNS tinyint  AS
BEGIN
	DECLARE @ISPrivate tinyint=0;
	SELECT @ISPrivate=IsPrivate From  Formulas WHERE FormulaId= @FormulaId;
	RETURN ISNULL(@ISPrivate,0);
END;










GO

/****** Object:  UserDefinedFunction [dbo].[FN_CompareTwoUsers]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_CompareTwoUsers]( @UserId1 int, @UserId2  int ) RETURNS tinyint AS
BEGIN
	DECLARE @RET tinyint=0;
	IF @UserId1 = @UserId2
		SET @RET=0
	ELSE
	 
	SELECT	
		@RET = COUNT(DISTINCT UG.UserGroupId)
	FROM Users U
	LEFT JOIN UserGroups UG ON U.UserGroupId = UG.UserGroupId
	WHERE UserId in (@UserId1, @UserId2);
	RETURN @RET
END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_ConfilctScheduleDate]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_ConfilctScheduleDate]( @FromDate DATE, @ToDate DATE, @FormulaId INT ) RETURNS INT AS
BEGIN
--DECLARE @FromDate DATE='2014-08-01', @ToDate DATE='2014-08-03', @FormulaId INT =103377 ;
	DECLARE @RET int=0;
	SELECT TOP 1 @RET= FormulaId FROM ImportSchedule 
	WHERE ImportStatus =0 AND @FormulaId = FormulaId AND
		( 
		(FromDateM  BETWEEN @FromDate AND @ToDate OR ToDateM BETWEEN @FromDate AND @ToDate)
		OR
		(@FromDate  BETWEEN FromDateM AND ToDateM OR @ToDate BETWEEN FromDateM AND ToDateM)
		)
	RETURN @RET
END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_ConflictScheduleDate]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_ConflictScheduleDate]( @FromDate DATE, @ToDate DATE, @FormulaId INT ) RETURNS INT AS
BEGIN
--DECLARE @FromDate DATE='2014-08-01', @ToDate DATE='2014-08-03', @FormulaId INT =103377 ;
	DECLARE @RET int=0;
	SELECT TOP 1 @RET= ImportScheduleId FROM ImportSchedule 
	WHERE ImportStatus =0 AND @FormulaId = FormulaId AND
		( 
		(FromDateM  BETWEEN @FromDate AND @ToDate OR ToDateM BETWEEN @FromDate AND @ToDate)
		OR
		(@FromDate  BETWEEN FromDateM AND ToDateM OR @ToDate BETWEEN FromDateM AND ToDateM)
		)
	RETURN @RET
END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_Get_Next_Formula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Get_Next_Formula]( @Data nvarchar(100), @SeqNo int)  RETURNS int AS
BEGIN
	Declare @FormulaId int=0;
	SELECT TOP 1 @FormulaId=FormulaId
	FROM VW_Select_Formulas  
	WHERE SeqNO > @SeqNo AND
				(
				Code						LIKE '%'+@Data+'%'	OR 
				SeqNO					LIKE '%'+@Data+'%'	OR 
				FormulaTypeDesc	LIKE '%'+@Data+'%'	OR 
				Name					LIKE '%'+@Data+'%'	OR 
				GeneralTitle			LIKE '%'+@Data+'%'	OR 
				IncludeLedgers		LIKE '%'+@Data+'%'	OR 
				UserName				LIKE '%'+@Data+'%'	OR 
				Title						LIKE '%'+@Data+'%'	OR 
				GeneralTitle			LIKE '%'+@Data+'%'	OR 
				IncludeLedgers		LIKE '%'+@Data+'%' 
				)
	ORDER BY SeqNO
	RETURN @FormulaId
END;


GO

/****** Object:  UserDefinedFunction [dbo].[FN_GetColumnList]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_GetColumnList]( @TableName nvarchar(4000), @Braket bit =NULL , @IncludeIdentity bit= 0 ) Returns nvarchar(1000) AS
BEGIN

	DECLARE @ColumnList nvarchar(4000)='';
		
	SET @TableName = REPLACE( REPLACE( @TableName ,'[','') ,']','') 

	SELECT 
		@ColumnList = @ColumnList + 
					CASE ISNULL(@Braket,0) 
						WHEN 0 THEN ','+ COLUMN_NAME ELSE ',['+ COLUMN_NAME + ']'
					END
	FROM INFORMATION_SCHEMA.COLUMNS  
	WHERE 
		TABLE_NAME = @TableName AND
		(ISNULL(@IncludeIdentity,0)=0 OR  COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') <> 1)
	ORDER BY ORDINAL_POSITION
	SET @ColumnList=REPLACE(','+@ColumnList,',,','')
	
	RETURN( @ColumnList );
END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_GetFormulaID]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_GetFormulaID](@formula_Code nvarchar(100)) Returns int AS
BEGIN
	DECLARE @FormulaID int=0;
	SELECT @FormulaID= FormulaId FROM Formulas
	WHERE  code = 'F10' --Cast( @formula_Code  );
	Return @FormulaID
END









GO

/****** Object:  UserDefinedFunction [dbo].[FN_IsAccessPrivateFormula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_IsAccessPrivateFormula]( @FormulaId int, @UserId int ) RETURNS tinyint AS
BEGIN
	DECLARE @RET tinyint=0, @OwnerId int=0;
	SELECT @RET = ISNULL(IsPrivate,0), @OwnerId = UserId FROM Formulas WHERE FormulaId= @FormulaId;
	SELECT @RET = ISNULL(@RET,0); 
	IF @RET = 1
		SELECT @RET = dbo.FN_CompareTwoUsers(@UserId , @OwnerId)
	RETURN @RET 
END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_IsTableLocked]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_IsTableLocked]( @TableName nvarchar(100)) RETURNS BIT  AS
BEGIN
	DECLARE @RET BIT =0;
	IF EXISTS(
				SELECT  1
				FROM   sys.dm_tran_locks	LCK
				INNER JOIN sys.partitions	PAR	ON LCK.resource_associated_entity_id = PAR.hobt_id
				WHERE object_name(PAR.object_id) = @TableName
			)
	SET @RET = 1
	

	RETURN @RET

END;










GO

/****** Object:  UserDefinedFunction [dbo].[FN_IsTableLockedSerializable]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_IsTableLockedSerializable]( @TableName nvarchar(100)) RETURNS BIT  AS
BEGIN
	DECLARE @RET BIT =0;
	IF EXISTS(
				SELECT 1					
				FROM   sys.dm_tran_locks   AS L
				INNER JOIN	sys.partitions AS P	ON L.resource_associated_entity_id = p.hobt_id
				INNER JOIN	sys.dm_exec_sessions S On L.resource_database_id = S.database_id and L.request_session_id= S.session_id
				WHERE 
					transaction_isolation_level=4 AND 
					Status ='running' AND 
					open_transaction_count =1 AND 
					object_name(P.object_id) = @TableName
			)
	SET @RET = 1

	RETURN @RET
	/*
			SELECT
				trans.session_id as [Session ID],
				trans.transaction_id as [Transaction ID],
				tas.name as [Transaction Name],
				tds.database_id as [Database ID]
			FROM sys.dm_tran_active_transactions tas
			INNER JOIN sys.dm_tran_database_transactions tds ON (tas.transaction_id = tds.transaction_id )
			INNER JOIN sys.dm_tran_session_transactions trans ON (trans.transaction_id=tas.transaction_id)
			WHERE 
				trans.is_user_transaction = 1 -- user
				AND tas.transaction_state = 2-- active
				AND tds.database_transaction_begin_time IS NOT NULL
	*/
END;










GO

/****** Object:  UserDefinedFunction [dbo].[FN_MiladiToShamsi]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_MiladiToShamsi](@MiladiDate Date) Returns Char(10) AS
BEGIN
	DECLARE @Shamsi Char(10);
	SELECT @Shamsi=Shamsi FROM TblDate
	WHERE MiladiDate = Cast( @MiladiDate as Date );

	Return @Shamsi
END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_MiladiToShamsiInt]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_MiladiToShamsiInt](@MiladiDate Date) Returns int AS
BEGIN
	DECLARE @Shamsiint int;
	SELECT @Shamsiint=Shamsiint FROM TblDate
	WHERE MiladiDate = Cast( @MiladiDate as Date );

	Return @Shamsiint
END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_RemoveCommentsInFormula]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_RemoveCommentsInFormula]( @Formula as nvarchar(max), @FormulaId as int ) RETURNS nvarchar(max) AS
BEGIN
	DECLARE @Idx1 int=0, @Idx2 int=0, @Idx3 int =0, @FormulaValue as nvarchar(max)='';
	IF @FormulaId > 0  
		SELECT @FormulaValue = Context FROM Formulas WHERE FormulaId=@FormulaId
	ELSE
		SET @FormulaValue =	@Formula;

	WHILE 1=1 
	BEGIN
		SET @Idx3 = 0;
		SELECT @Idx1= CHARINDEX('/*',@FormulaValue,1), @Idx2 = CHARINDEX('*/',@FormulaValue,1), @Idx3= CHARINDEX('/*',@FormulaValue, CHARINDEX('/*',@FormulaValue,1)+1)
		IF (@Idx1=0 and @Idx2>0) OR (@Idx1>=@Idx2-1) OR ( @Idx3 >0 AND @Idx3<@Idx2) 
			BREAK
		SET @FormulaValue= REPLACE(@FormulaValue,SUBSTRING( @FormulaValue, @Idx1, @Idx2- @Idx1+2),'')
	
	END
	IF @Idx1+@Idx2+@Idx3 = 0 
		SET @FormulaValue = NULLIF(@FormulaValue,'') 
	ELSE
		SET @FormulaValue = '<ERROR IN COMMENT>' ;
	RETURN @FormulaValue 

END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_RemoveCommentsInFormulaCompressed]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_RemoveCommentsInFormulaCompressed]( @Formula as nvarchar(max), @FormulaId as int ) RETURNS nvarchar(max) AS
BEGIN
	DECLARE @Idx1 int, @Idx2 int, @Idx3 int , @FormulaValue as nvarchar(max)='';
	IF @FormulaId > 0  
		SELECT @FormulaValue = Context FROM Formulas WHERE FormulaId=@FormulaId
	ELSE
		SET @FormulaValue =	@Formula;

	SELECT @Idx1 =0, @Idx2 =0, @Idx3 =0;
	WHILE 1=1 
	BEGIN
		SET @Idx3 = 0;
		SELECT @Idx1= CHARINDEX('/*',@FormulaValue,1), @Idx2 = CHARINDEX('*/',@FormulaValue,1), @Idx3= CHARINDEX('/*',@FormulaValue, CHARINDEX('/*',@FormulaValue,1)+1)
		IF (@Idx1=0 and @Idx2>0) OR (@Idx1>=@Idx2-1) OR ( @Idx3 >0 AND @Idx3<@Idx2) 
			BREAK
		SET @FormulaValue= REPLACE(@FormulaValue,SUBSTRING( @FormulaValue, @Idx1, @Idx2- @Idx1+2),'')
	
	END

	IF @Idx1+@Idx2+@Idx3 <> 0 
		RETURN '<ERROR IN COMMENT>';
	
	SELECT @Idx1 =0, @Idx2 =0, @Idx3 =0;
	WHILE 1=1 
	BEGIN
		SET @Idx2 = 0;
		SELECT 
			@Idx1= CHARINDEX('--',@FormulaValue,1), 
			@Idx2= CHARINDEX(CHAR(13),@FormulaValue, CHARINDEX('--',@FormulaValue,1)+1),
			@Idx3= LEN(@FormulaValue)

		IF @Idx1=0 
			BREAK
		IF @Idx2=0
			SET @Idx2= @Idx3
		SET @FormulaValue= REPLACE(@FormulaValue,SUBSTRING( @FormulaValue, @Idx1, @Idx2- @Idx1+2),'')
	
	END

	SELECT @Idx1 = LEN(@FormulaValue)
	WHILE @Idx1 >=1 
	BEGIN
		IF ASCII(SUBSTRING( @FormulaValue, @Idx1 , 1 ))>127
			SET @FormulaValue= STUFF(@FormulaValue, @Idx1, 1,'')
		SET @Idx1 = @Idx1 -1
	END

	RETURN NULLIF(REPLACE(REPLACE(REPLACE(REPLACE(@FormulaValue, CHAR(10),''),CHAR(13),''),' ',''),CHAR(9),'') ,'')

END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_ShamsiToMiladi]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_ShamsiToMiladi](@Shamsi Char(10)) Returns Char(10) AS
BEGIN
	DECLARE @Miladi Char(10)='';
	SELECT @Miladi= Miladi FROM TblDate
	WHERE  Shamsi = @Shamsi 
	Return @Miladi
END










GO

/****** Object:  UserDefinedFunction [dbo].[FN_ShamsiToMiladiDate]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_ShamsiToMiladiDate](@Shamsi Char(10)) Returns Date AS
BEGIN
	DECLARE @MiladiDate Date;
	SELECT @MiladiDate= CAST(MiladiDate as Date) FROM TblDate
	WHERE  Shamsi = @Shamsi 
	Return @MiladiDate
END











GO

/****** Object:  UserDefinedFunction [dbo].[FN_UserIsAdmin]    Script Date: 7/26/2020 1:32:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_UserIsAdmin]( @UserId int ) RETURNS tinyint AS
BEGIN
	DECLARE @RET tinyint =0;
	SELECT @RET = ISNULL(IsAdmin,0) From Users WHERE UserId= @UserId;
	RETURN ( @RET ) 
END;











GO


