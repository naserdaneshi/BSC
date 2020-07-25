USE [BSC]
GO

/****** Object:  UserDefinedFunction [dbo].[FN_Select_Drived_FormulaFromByCode]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[FN_Select_Drived_FormulaFromByCode]( @Code nvarchar(100) ) 
	RETURNS @Ret TABLE(FormulaType tinyint, SeqNO int, Code nvarchar(100), Context nvarchar(4000)) 
AS
BEGIN
	DECLARE @CTE TABLE (FormulaType int, Code varchar(50), SeqNO int, UsedCode varchar(100));
	INSERT @CTE SELECT FormulaType,Code, SeqNO, UsedCode FROM VW_SeperatedItemFormulas  
	;WITH CTE_REC AS
	(
		SELECT 0 as LV ,FormulaType,Code,SeqNO,UsedCode FROM @CTE  
		WHERE   Code =@Code 
		UNION ALL
		SELECT B.LV+1, A.FormulaType,A.Code,A.SeqNO, A.UsedCode FROM @CTE A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	)
	INSERT INTO @Ret	SELECT DISTINCT
		A.FormulaType, F.SeqNO , A.Code, Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
	FROM CTE_REC A
	INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
	ORDER BY F.SeqNO;

	RETURN
END



GO

/****** Object:  UserDefinedFunction [dbo].[FN_Select_Drived_FormulaToByCode]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[FN_Select_Drived_FormulaToByCode]( @Code nvarchar(100) ) 
	RETURNS @Ret TABLE(FormulaType tinyint, SeqNO int, Code nvarchar(100), Context nvarchar(4000)) 
AS
BEGIN
	DECLARE @CTE TABLE (FormulaType int, Code varchar(50), SeqNO int, UsedCode varchar(100));
	INSERT @CTE SELECT FormulaType,Code, SeqNO, UsedCode FROM VW_SeperatedItemFormulas  
	
	;WITH CTE_REC AS
	(
		SELECT 0 as LV ,FormulaType,Code,SeqNO,UsedCode FROM @CTE  
		WHERE Code =@Code 
		UNION ALL
		SELECT B.LV+1, A.FormulaType,A.Code,A.SeqNO, A.UsedCode FROM @CTE A
		INNER JOIN CTE_REC B ON B.Code = A.UsedCode   

	)
	INSERT INTO @Ret
	SELECT 
	DISTINCT
		A.FormulaType, 
		F.SeqNO ,
		A.Code, Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context

	FROM CTE_REC A
	INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
	ORDER BY F.SeqNO;

	RETURN
END




GO

/****** Object:  UserDefinedFunction [dbo].[FN_SringToTableContext]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_SringToTableContext]( @Context nvarchar(max) ) RETURNS @RET TABLE( RowNO INT, Item nvarchar(100) ) AS
BEGIN
	--Declare @Context nvarchar(max)='[GL_4040_2_1]-[GL_4040_2_2]+[GL_4040_2_3]-[GL_4040_2_4]+[GL_4040_2_5]*[GL_4040_2_6]+[GL_4040_2_7]+[GL_4040_2_8]+[GL_4040_2_9]/[GL_4040_2_10]+[GL_4040_2_11]-[GL_4040_2_12]+[GL_4040_2_13]-[GL_4040_2_14]+[GL_4040_2_15]-[GL_4040_2_16]+[GL_4040_2_17]+[GL_4040_2_18]+[GL_4040_2_19]+[GL_4040_2_20]+[GL_4040_2_21]+[GL_4040_2_22]+[GL_4040_2_23]+[GL_4040_2_24]+[GL_4040_2_25]+[GL_4040_2_26]+[GL_4040_2_27]+[GL_4040_2_28]+[GL_4040_2_29]+[GL_4040_2_30]+[GL_4040_2_31]+[GL_4040_2_32]+[GL_4040_2_33]+[GL_4040_2_34]+[GL_4040_2_35]+[GL_4040_2_36]+[GL_4040_2_57]+[GL_4040_2_58]+[GL_4040_2_59]+[GL_4040_2_37]+[GL_4040_2_99]';
	SET @Context = '+'+@Context;
	Declare @pString nvarchar(max)=REPLACE(REPLACE(REPLACE(@Context,'*','+'),'-','+'),'/','+'), @Delimiter CHAR(1)='+' ;
	
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  )
	,RowCounter AS -- 271737 Rows
		(SELECT TOP(LEN(@pString)+1) ROW_NUMBER()OVER( ORDER BY (SELECT NULL) )  as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	, Locations AS
	(
	SELECT  TOP 1000000000
		SUBSTRING( @Context, RowNO, ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0), LEN(@pString)+1)-RowNO) as Item
	FROM RowCounter 
	WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	ORDER BY 1
	)
	INSERT INTO @RET
	SELECT 
		RowNO = ROW_NUMBER() OVER( ORDER BY Item), Item 
	FROM Locations

	RETURN
END	




GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTable]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTable]( @pString nvarchar(max) , @Delimiter CHAR(1)) 
							RETURNS @RET TABLE( RowNO INT, Item nvarchar(4000) ) AS
BEGIN
	SET @pString= @Delimiter+@pString;
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  UNION ALL SELECT 1 )
	,RowCounter AS -- 271737 Rows
		(SELECT 1 as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	,TopRows  AS 
		(	
			SELECT TOP(LEN(@pString)+1) 
				'RowNO'=ROW_NUMBER()OVER( ORDER BY (SELECT NULL)  ) 
			FROM RowCounter  
		) 
	,Locations(RowNO,Pos) AS
	(	
		SELECT  
			RowNO+1 , ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0) , LEN(@pString)+1) -1
		FROM TopRows 
		WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	)
	INSERT INTO @RET
	SELECT 
		RowNO = ROW_NUMBER() OVER(ORDER BY RowNO ),
		Item= NULLIF(SUBSTRING( @pString, RowNO, Pos - RowNO+1 ),'')
	FROM Locations 
	WHERE Pos > RowNO
	RETURN
END




GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTable_DISTINCT]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTable_DISTINCT]( @pString nvarchar(max) , @Delimiter CHAR(1)=',') 
							RETURNS @RET TABLE( RowNO INT, Item nvarchar(4000) ) AS
BEGIN
	SET @pString= @Delimiter+@pString;
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  )
	,RowCounter AS -- 271737 Rows
		(SELECT 1 as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	,TopRows  AS 
		(	
			SELECT TOP(LEN(@pString)+1) 
				'RowNO'=ROW_NUMBER()OVER( ORDER BY (SELECT NULL)  ) 
			FROM RowCounter  
		) 
	,Locations(RowNO,Pos) AS
	(	
		SELECT  
			RowNO+1 , ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0) , LEN(@pString)+1) -1
		FROM TopRows 
		WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	)
	, CTE AS
	(
		SELECT DISTINCT 
			Item  = NULLIF(SUBSTRING( @pString, RowNO, Pos - RowNO+1 ),'')
		FROM Locations 
	)
	INSERT INTO @RET
	SELECT RowNO = ROW_NUMBER() OVER(ORDER BY ROWNO),Item  
	FROM 
	(
		SELECT 
			RowNO = ROW_NUMBER() OVER(ORDER BY (SELECT NULL) ),Item 
		FROM CTE 
	) K
	WHERE Item IS NOT NULL

	RETURN
END






GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTable_Not_NULL]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTable_Not_NULL]( @pString nvarchar(max) , @Delimiter CHAR(1)=',' ) 
							RETURNS @RET TABLE( RowNO INT, Item nvarchar(4000) ) AS
BEGIN
	SET @pString= @Delimiter+@pString;
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  )
	,RowCounter AS -- 271737 Rows
		(SELECT 1 as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	,TopRows  AS 
		(	
			SELECT TOP(LEN(@pString)+1) 
				'RowNO'=ROW_NUMBER()OVER( ORDER BY (SELECT NULL)  ) 
			FROM RowCounter  
		) 
	,Locations(RowNO,Pos) AS
	(	
		SELECT  
			RowNO+1 , ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0) , LEN(@pString)+1) -1
		FROM TopRows 
		WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	)
	INSERT INTO @RET
	SELECT RowNO = ROW_NUMBER() OVER(ORDER BY RowNO ),Item
	FROM(
			SELECT 
				RowNO = ROW_NUMBER() OVER(ORDER BY RowNO ),
				Item= NULLIF(SUBSTRING( @pString, RowNO, Pos - RowNO+1 ),'')
			FROM Locations 
		) K
	WHERE K.Item IS NOT NULL

	RETURN
END



GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTableFormula]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTableFormula]( @Context nvarchar(max) )RETURNS @RET TABLE( RowNO INT, Item nvarchar(100) ) AS
BEGIN
	--Declare @Context nvarchar(max)='[GL_4040_2_1]-[GL_4040_2_2]+[GL_4040_2_3]-[GL_4040_2_4]+[GL_4040_2_5]*[GL_4040_2_6]+[GL_4040_2_7]+[GL_4040_2_8]+[GL_4040_2_9]/[GL_4040_2_10]+[GL_4040_2_11]-[GL_4040_2_12]+[GL_4040_2_13]-[GL_4040_2_14]+[GL_4040_2_15]-[GL_4040_2_16]+[GL_4040_2_17]+[GL_4040_2_18]+[GL_4040_2_19]+[GL_4040_2_20]+[GL_4040_2_21]+[GL_4040_2_22]+[GL_4040_2_23]+[GL_4040_2_24]+[GL_4040_2_25]+[GL_4040_2_26]+[GL_4040_2_27]+[GL_4040_2_28]+[GL_4040_2_29]+[GL_4040_2_30]+[GL_4040_2_31]+[GL_4040_2_32]+[GL_4040_2_33]+[GL_4040_2_34]+[GL_4040_2_35]+[GL_4040_2_36]+[GL_4040_2_57]+[GL_4040_2_58]+[GL_4040_2_59]+[GL_4040_2_37]+[GL_4040_2_99]';
	SET @Context = '+'+@Context;
	Declare @pString nvarchar(max)=REPLACE(REPLACE(REPLACE(@Context,'*','+'),'-','+'),'/','+'), @Delimiter CHAR(1)='+' ;
	
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  )
	,RowCounter AS -- 271737 Rows
		(SELECT TOP(LEN(@pString)+1) ROW_NUMBER()OVER( ORDER BY (SELECT NULL) )  as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	, Locations AS
	(
	SELECT  TOP 1000000000
		SUBSTRING( @Context, RowNO, ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0), LEN(@pString)+1)-RowNO) as Item
	FROM RowCounter 
	WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	ORDER BY 1
	)
	INSERT INTO @RET
	SELECT 
		RowNO = ROW_NUMBER() OVER( ORDER BY Item), Item 
	FROM Locations

	RETURN
END



GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTableTiny]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTableTiny]
							( @pString nvarchar(max) , @Delimiter CHAR(1)=',') 
							RETURNS @RET TABLE( RowNO INT, Item nvarchar(25) ) AS
BEGIN
	SET @pString= @Delimiter+@pString;
	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  UNION ALL SELECT 1 )
	,RowCounter AS -- 271737 Rows
		(SELECT 1 as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g)
	,TopRows  AS 
		(	
			SELECT TOP(LEN(@pString)+1) 
				'RowNO'=ROW_NUMBER()OVER( ORDER BY (SELECT NULL)  ) 
			FROM RowCounter  
		) 
	,Locations(RowNO,Pos) AS
	(	
		SELECT  
			RowNO+1 , ISNULL(NULLIF(CHARINDEX( @Delimiter ,@pString, RowNO+1),0) , LEN(@pString)+1) -1
		FROM TopRows 
		WHERE SUBSTRING(@pString, RowNO,1)= @Delimiter  
	)
	INSERT INTO @RET
	SELECT RowNO = ROW_NUMBER() OVER(ORDER BY RowNO ),Item
	FROM(
			SELECT 
				RowNO = ROW_NUMBER() OVER(ORDER BY RowNO ),
				Item= NULLIF(SUBSTRING( @pString, RowNO, Pos - RowNO+1 ),'')
			FROM Locations 
		) K
	WHERE K.Item IS NOT NULL

	RETURN
END




GO

/****** Object:  UserDefinedFunction [dbo].[FN_Import_FetchDates]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Import_FetchDates]( @FetchDate as Date ) RETURNS TABLE AS RETURN (
-- DECLARE @FetchDate as Date='2015-11-23'
	WITH CTE_Dates AS
	(
		SELECT 
			DT.MiladiDate, DT.Shamsi
		FROM TblDate DT
		LEFT JOIN Reports RP ON DT.MiladiDate = RP.FetchDateM
		WHERE 
			( DT.MiladiDate BETWEEN ( SELECT TOP 1 StartFetchDateM FROM Configs ) AND @FetchDate ) AND 
			( RP.FetchDateM IS NULL OR ISNULL(RP.Retry,0) =1 )	
		UNION
		SELECT	
			DT.MiladiDate, DT.Shamsi
		FROM ImportSchedule SC
		INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0 
	)

	SELECT
		TOP 1000000000
		ROW_NUMBER() OVER( ORDER BY MiladiDate) as RW, 
		MiladiDate, Shamsi
	FROM	CTE_Dates A
	ORDER BY MiladiDate
);











GO

/****** Object:  UserDefinedFunction [dbo].[FN_Import_FormulasFetchDates]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Import_FormulasFetchDates] ( @FetchDate as Date ) RETURNS TABLE AS RETURN (
	--DECLARE @FetchDate as Date = '2015-11-23';
	--»« œ— ‰Ÿ— ê—› ‰  «—ÌŒ œ—ŒÊ«”  „Õ«”»Â „Ãœœ ‰“Ìò —Ì‰ ›—„Ê· —« ÅÌœ« „Ìò‰œ
	WITH CTE_Formula_LogDates AS
	(
		SELECT	DISTINCT
			DT.MiladiDate, DT.Shamsi, LF.FormulaId, SeqNO, FormulaType, Code, GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
		FROM ImportSchedule SC
		INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0 AND SC.ImportMode = 1 -- Backward
		LEFT  JOIN LOG_Formulas LF ON LF.FormulaId = SC.FormulaId  AND ActType in('Inserted','UInserted')
		WHERE ActionDate = ( SELECT MAX(ActionDate)  FROM LOG_Formulas WHERE LF.FormulaId = FormulaId and CAST(ActionDate as DATE) <= MiladiDate )
	)	
	,CTE_Formula_LastDates AS
	(
		SELECT 
			DT.MiladiDate, DT.Shamsi, F.FormulaId 
		FROM TblDate DT
		LEFT JOIN Reports RP ON DT.MiladiDate = RP.FetchDateM
		INNER JOIN Formulas F ON 1=1
		WHERE 
			( DT.MiladiDate BETWEEN ( SELECT TOP 1 StartFetchDateM FROM Configs ) AND @FetchDate ) AND 
			( RP.FetchDateM IS NULL OR ISNULL(RP.Retry,0) =1 )	
		UNION
		SELECT
			DT.MiladiDate, DT.Shamsi, SC.FormulaId
		FROM ImportSchedule SC
		INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0 AND SC.ImportMode = 0
	)
	,CTE AS
	(
		SELECT 
			0 as Mode, MiladiDate, Shamsi, A.FormulaId, SeqNO, FormulaType, Code, GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context 
		FROM	CTE_Formula_LastDates A
		INNER JOIN Formulas F ON F.FormulaId = A.FormulaId 
		WHERE NOT EXISTS( SELECT 1 FROM CTE_Formula_LogDates WHERE FormulaId = A.FormulaId and MiladiDate = A.MiladiDate)
		UNION
		SELECT 
			1 as Mode, MiladiDate, Shamsi, FormulaId, SeqNO, FormulaType, Code, GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context 
		FROM	CTE_Formula_LogDates A
	)
	SELECT	TOP 10000000000
		ROW_NUMBER() OVER( ORDER BY SeqNO, FormulaId, MiladiDate) RowId,
		Mode, MiladiDate, Shamsi, FormulaId, SeqNO, FormulaType, Code, 
		GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
	FROM CTE A
	WHERE MiladiDate = ( SELECT MAX(MiladiDate) FROM CTE WHERE A.FormulaId = FormulaId ) --and MiladiDate >= A.MiladiDate and ) 
	ORDER BY RowId 
)











GO

/****** Object:  UserDefinedFunction [dbo].[FN_Import_FormulasInDate]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Import_FormulasInDate] ( @FetchDate as Date ) RETURNS TABLE AS RETURN (
	--DECLARE @FetchDate as Date = '2015-12-23';
	--»« œ— ‰Ÿ— ê—› ‰  «—ÌŒ œ—ŒÊ«”  „Õ«”»Â „Ãœœ ‰“Ìò —Ì‰ ›—„Ê· —« ÅÌœ« „Ìò‰œ

	WITH CTE_Formula_LogDates AS
	(
		SELECT	
			ActionDate,
			IIF(LF.FormulaId IS NOT NULL,	1				,	0				) as Mode,
			IIF(LF.FormulaId IS NOT NULL,	LF.CreateDateM	,	F.CreateDateM	) as MiladiDate,
			IIF(LF.FormulaId IS NOT NULL,	LF.CreateDateS	,	F.CreateDateS	) as Shamsi,
			IIF(LF.FormulaId IS NOT NULL,	LF.FormulaId	,	F.FormulaId		) as FormulaId,
			IIF(LF.FormulaId IS NOT NULL,	LF.SeqNO		,	F.SeqNO			) as SeqNO,
			IIF(LF.FormulaId IS NOT NULL,	LF.FormulaType	,	F.FormulaType	) as FormulaType,
			IIF(LF.FormulaId IS NOT NULL,	LF.Code			,	F.Code			) as Code,
			IIF(LF.FormulaId IS NOT NULL,	LF.GeneralId	,	F.GeneralId		) as GeneralId,
			IIF(LF.FormulaId IS NOT NULL,	LF.FromLedger	,	F.FromLedger	) as FromLedger,
			IIF(LF.FormulaId IS NOT NULL,	LF.ToLedger		,	F.ToLedger		) as ToLedger,
			IIF(LF.FormulaId IS NOT NULL,	LF.UsedLedgers	,	F.UsedLedgers	) as UsedLedgers,
			IIF(LF.FormulaId IS NOT NULL,	LF.Context		,	F.Context		) as Context,
			IIF(LF.FormulaId IS NOT NULL,	LF.UsedLedgersAbr	,	F.UsedLedgersAbr	) as UsedLedgersAbr,
			IIF(LF.FormulaId IS NOT NULL,	LF.BranchesIdList	,	F.BranchesIdList	) as BranchesIdList
		FROM  Formulas F
		LEFT JOIN ImportSchedule	SC	ON F.FormulaId = SC.FormulaId AND @Fetchdate BETWEEN FromDateM AND ToDateM and ImportStatus = 0
		LEFT JOIN LOG_Formulas		LF	ON 
				LF.FormulaId = F.FormulaId AND 
				ActType in('UInserted','Inserted') AND 
				CAST( ActionDate as DATE ) = CAST(
				ISNULL (
						(SELECT MAX(ActionDate) FROM LOG_Formulas WHERE LF.FormulaId = FormulaId AND ( CAST(ActionDate as DATE)<= @Fetchdate OR ImportMode =0 ) )
						,(SELECT MIN(ActionDate) FROM LOG_Formulas WHERE LF.FormulaId = FormulaId) 
						) AS DATE)
	),
	CTE2 AS
	(
		SELECT
			ROW_NUMBER() OVER( ORDER BY SeqNO, FormulaId, MiladiDate) RowId,
			ROW_NUMBER() OVER( PARTITION BY FormulaId ORDER BY ActionDate DESC ) Row2,
			ActionDate, Mode, FormulaId, SeqNO, FormulaType, Code, 
			GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
		FROM CTE_Formula_LogDates
	)
	SELECT TOP 10000000000
			RowId,
			ActionDate, Mode, FormulaId, SeqNO, FormulaType, Code, 
			GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
	FROM CTE2		
	WHERE Row2 =1
	ORDER BY RowId 
)




GO

/****** Object:  UserDefinedFunction [dbo].[FN_Import_FormulasInDateTemp]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Import_FormulasInDateTemp] ( @FetchDate as Date ) RETURNS TABLE AS RETURN (
	--DECLARE @FetchDate as Date = '2015-12-23';
	--»« œ— ‰Ÿ— ê—› ‰  «—ÌŒ œ—ŒÊ«”  „Õ«”»Â „Ãœœ ‰“Ìò —Ì‰ ›—„Ê· —« ÅÌœ« „Ìò‰œ

	WITH CTE_Formula_LogDates AS
	(
		SELECT	
			ActionDate,
			IIF(LF.FormulaId IS NOT NULL,	1				,	0				) as Mode,
			IIF(LF.FormulaId IS NOT NULL,	LF.CreateDateM	,	F.CreateDateM	) as MiladiDate,
			IIF(LF.FormulaId IS NOT NULL,	LF.CreateDateS	,	F.CreateDateS	) as Shamsi,
			IIF(LF.FormulaId IS NOT NULL,	LF.FormulaId	,	F.FormulaId		) as FormulaId,
			IIF(LF.FormulaId IS NOT NULL,	LF.SeqNO		,	F.SeqNO			) as SeqNO,
			IIF(LF.FormulaId IS NOT NULL,	LF.FormulaType	,	F.FormulaType	) as FormulaType,
			IIF(LF.FormulaId IS NOT NULL,	LF.Code			,	F.Code			) as Code,
			IIF(LF.FormulaId IS NOT NULL,	LF.GeneralId	,	F.GeneralId		) as GeneralId,
			IIF(LF.FormulaId IS NOT NULL,	LF.FromLedger	,	F.FromLedger	) as FromLedger,
			IIF(LF.FormulaId IS NOT NULL,	LF.ToLedger		,	F.ToLedger		) as ToLedger,
			IIF(LF.FormulaId IS NOT NULL,	LF.UsedLedgers	,	F.UsedLedgers	) as UsedLedgers,
			IIF(LF.FormulaId IS NOT NULL,	LF.Context		,	F.Context		) as Context,
			IIF(LF.FormulaId IS NOT NULL,	LF.UsedLedgersAbr	,	F.UsedLedgersAbr	) as UsedLedgersAbr,
			IIF(LF.FormulaId IS NOT NULL,	LF.BranchesIdList	,	F.BranchesIdList	) as BranchesIdList
		FROM  Formulas F
		LEFT JOIN ImportSchedule	SC	ON F.FormulaId = SC.FormulaId AND @Fetchdate BETWEEN FromDateM AND ToDateM and ImportStatus = 0
		LEFT JOIN LOG_Formulas		LF	ON 
				LF.FormulaId = F.FormulaId AND 
				ActType in('UInserted','Inserted') AND 
				CAST( ActionDate as DATE ) = CAST(
				ISNULL (
						(SELECT MAX(ActionDate) FROM LOG_Formulas WHERE LF.FormulaId = FormulaId AND ( CAST(ActionDate as DATE)<= @Fetchdate OR ImportMode =0 ) )
						,(SELECT MIN(ActionDate) FROM LOG_Formulas WHERE LF.FormulaId = FormulaId) 
						) AS DATE)
	),
	CTE2 AS
	(
		SELECT
			ROW_NUMBER() OVER( ORDER BY SeqNO, FormulaId, MiladiDate) RowId,
			ROW_NUMBER() OVER( PARTITION BY FormulaId ORDER BY ActionDate DESC ) Row2,
			ActionDate, Mode, FormulaId, SeqNO, FormulaType, Code, 
			GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
		FROM CTE_Formula_LogDates
	)
	SELECT TOP 10000000000
			RowId,
			ActionDate, Mode, FormulaId, SeqNO, FormulaType, Code, 
			GeneralId, FromLedger, ToLedger, UsedLedgers, UsedLedgersAbr, BranchesIdList, Context
	FROM CTE2		
	WHERE Row2 =1
	ORDER BY RowId 
)




GO

/****** Object:  UserDefinedFunction [dbo].[FN_Select_Drived_Formula]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Select_Drived_Formula] () RETURNS TABLE AS 
RETURN(
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			FormulaId,SeqNo, Code, '['+Code+']' as Code2, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	(
		SELECT 
			FormulaId,SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY dbo.FN_StringToTable_Not_NULL( UsedLedgersAbr2 ,',') A
	)	
	SELECT FormulaId,SeqNo,Code,Code2,Lst 
	FROM CTE_ALL A
	WHERE LST IN ( SELECT CODE2 FROM CTE_ALL)
);











GO

/****** Object:  UserDefinedFunction [dbo].[FN_Select_Formulas_ByAccessibility]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_Select_Formulas_ByAccessibility](@UserId int )  RETURNS TABLE AS
RETURN
(
WITH CTE AS
(
	SELECT 
		UserId,UserGroupId,UserName
	FROM Users U
	WHERE UserGroupId in (SELECT UserGroupId FROM Users WHERE (UserId = @UserId OR @UserId =0) )
)

SELECT
	DISTINCT
	F.FormulaId, F.UserId
	
FROM Formulas F
INNER JOIN CTE B ON (B.UserId = F.UserId OR ISNULL(IsPrivate,0) =0) OR dbo.FN_UserIsAdmin(@UserId) =1
)










GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTableBracket]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTableBracket]( @pString nvarchar(max) ) RETURNS TABLE AS
RETURN(
	--Declare @pString nvarchar(4000) ='[F18]+[F20]+[F1]/[F10]+[F4]-[F15]+[F17]+[F7]';
 	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  UNION ALL SELECT 1 )
	,RowCounter AS -- 271737 Rows
		(SELECT ROW_NUMBER()OVER( ORDER BY (SELECT NULL))  as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	, Cte_STRING AS
	(
		Select REPLACE(REPLACE(REPLACE(@pString ,' ',''),CHAR(10),''),CHAR(13),'') as S
	),
	CTE AS
	(
		SELECT  
			RowNO,  S,SUBSTRING( S,RowNo,1) C
		FROM RowCounter RC
		INNER JOIN Cte_STRING A ON 1=1 
		WHERE RowNO <= LEN(S) 
	)
	,
	CTESort AS
	(
		SELECT TOP 100000
		  ROW_NUMBER() OVER( ORDER BY RowNO ) % 2  as RW, 
		  RowNO, S,SUBSTRING(S,RowNO, LEAD(RowNO) OVER( ORDER BY RowNO) -RowNO +1 ) Item
		FROM CTE
		WHERE C in('[',']')
		ORDER BY RowNO 
	)
	SELECT  TOP 100000000
		ROW_NUMBER() OVER( ORDER BY RowNO ) as RowNO,Item 
	FROM CTESort A
	WHERE RW = 1 
	ORDER BY 1
	)











GO

/****** Object:  UserDefinedFunction [dbo].[FN_StringToTableByPair]    Script Date: 7/26/2020 1:31:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FN_StringToTableByPair]( @pString nvarchar(max), @Open varchar(5) , @Close varchar(5) ) 
RETURNS TABLE AS
RETURN(
	--Declare @pString nvarchar(4000) ='[F18]+[F20]+[F1]/[F10]+[F4]-[F15]+[F17]+[F7]';

 	WITH Rows1 AS 
		( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  UNION ALL SELECT 1 )
	,RowCounter AS -- 271737 Rows
		(SELECT ROW_NUMBER()OVER( ORDER BY (SELECT NULL))  as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
	,Cte_STRING AS
	(
		Select REPLACE(REPLACE(REPLACE(@pString ,' ',''),CHAR(10),''),CHAR(13),'') as S
	),
	CTE AS
	(
		SELECT  
			RowNO,  S,SUBSTRING( S,RowNo,1) C
		FROM RowCounter RC
		INNER JOIN Cte_STRING A ON 1=1 
		WHERE RowNO <= LEN(S) 
	)
	,
	CTESort AS
	(
		SELECT TOP 100000
		  ROW_NUMBER() OVER( ORDER BY RowNO ) % 2  as RW, 
		  RowNO, S,SUBSTRING(S,RowNO, LEAD(RowNO) OVER( ORDER BY RowNO) -RowNO +1 ) Item
		FROM CTE
		WHERE C in(@Open,@Close)
		ORDER BY RowNO 
	)
	SELECT  TOP 100000000
		ROW_NUMBER() OVER( ORDER BY RowNO ) as RowNO,Item 
	FROM CTESort A
	WHERE RW = 1 
	ORDER BY 1
	)











GO


