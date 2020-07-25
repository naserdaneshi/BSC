USE [BSC]
GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormula]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormula] AS
BEGIN
	SET NOCOUNT ON;	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------

	TRUNCATE TABLE FormulaRemain;

	INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL(UsedLedgers,',')) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN Ledgers L ON L.LedgerId = A.Item 
	WHERE 
		FormulaType = 0 AND RT.FetchDateM in( SELECT FetchDateM FROM VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	WHERE 
		FormulaType = 1 AND RT.FetchDateM in( SELECT FetchDateM FROM VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	WHERE 
		FormulaType = 2 AND RT.FetchDateM in( SELECT FetchDateM FROM VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM;

	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FormulaId int, @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	FormulaId  from Formulas	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		EXEC USP_Generate_CalcSQL_ByFormula @FormulaId, @SQLText OUT;
		EXEC( @SQLText )
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula

	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas(BranchId,FetchDateM,Code,Remain,FirstRemain,LastRemain,CreditValue,DebitValue)
	SELECT 	BranchId,FetchDateM,Code,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM FormulaRemain;

END;







GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormulaBackward]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormulaBackward] AS
BEGIN
	DECLARE @FetchDateM Date;

	DECLARE Cur_Formula CURSOR	FOR  
			SELECT DISTINCT  DRT.FetchDateM 
			FROM DayResultTrans DRT
			INNER JOIN  Reports RP ON RP.FetchDateM = DRT.FetchDateM AND RP.Backward = 1;
							
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FetchDateM; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN
		DELETE ResultFormulas WHERE FetchDateM =  @FetchDateM;
		DELETE FormulaRemain  WHERE FetchDateM =  @FetchDateM;

		EXEC USP_CalculateAndTransferFormulaBackwardByDate @FetchDateM		

		--INSERT INTO ResultFormulas
		--SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
		--FROM FormulaRemain
		--WHERE FetchDateM= @FetchDateM;
		
		UPDATE CalculationLogs	SET Status=1, Backward=1 WHERE FetchDateM =  @FetchDateM;

		FETCH NEXT FROM Cur_Formula INTO @FetchDateM; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula

END;



GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormulaBackwardByDate]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormulaBackwardByDate]( @FetchDateM Date ) AS
BEGIN
	SET NOCOUNT ON;	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------
	--DECLARE @FetchDateM Date='2015-10-10';

	TRUNCATE TABLE FormulasBackward;
	--Select * from FormulasBackward

	INSERT INTO FormulasBackward(RowId,FormulaId,Mode,SeqNO,FormulaType,Code,GeneralId,FromLedger,ToLedger,UsedLedgers,UsedLedgersAbr,BranchesIdList,Context)
	SELECT 
		RowId,FormulaId,Mode,SeqNO,FormulaType,Code,GeneralId,FromLedger,ToLedger,UsedLedgers,UsedLedgersAbr,BranchesIdList,Context
	FROM  dbo.FN_Import_FormulasInDate(@FetchDateM);
	
	INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM FormulasBackward F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL(UsedLedgers,',') ) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN Ledgers L ON L.LedgerId = A.Item 
	WHERE 
		FormulaType = 0 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',') ))
	GROUP BY F.Code, BranchId, RT.FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM FormulasBackward F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	WHERE 
		FormulaType = 1 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',') ))
	GROUP BY F.Code, BranchId, RT.FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM FormulasBackward F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	WHERE 
		FormulaType = 2 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',') ))
	GROUP BY F.Code, BranchId, RT.FetchDateM;

	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FormulaId int, @SQLText nvarchar(MAX)='SELECT 1 as A ';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	FormulaId  from FormulasBackward	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		EXEC USP_Generate_CalcSQL_ByFormulaBackward @FormulaId, @FetchDateM, @SQLText OUT;
		EXEC( @SQLText )
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula

	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		@FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas(FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue)
	SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM FormulaRemain
	WHERE FetchDateM = @FetchDateM;
END




GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormulaRegular]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormulaRegular] AS
BEGIN
	SET NOCOUNT ON;	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------
	--exec dbo.SP_KillProcess;
	TRUNCATE TABLE FormulaRemain;
	TRUNCATE TABLE DateUsedInLastFetch;

	INSERT INTO DateUsedInLastFetch
	SELECT DISTINCT A.FetchDateM, ISNULL(RP.Backward,0) FROM VW_DateUsedInLastFetch A
	INNER JOIN Reports RP ON A.FetchDateM = RP.FetchDateM 

	INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL(UsedLedgers,',')) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN DateUsedInLastFetch RP ON RT.FetchDateM = RP.FetchDateM and Backward = 0
	INNER JOIN Ledgers L ON L.LedgerId = A.Item 
	WHERE 
		FormulaType = 0 AND 
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN DateUsedInLastFetch RP ON RT.FetchDateM = RP.FetchDateM and Backward = 0
	WHERE 
		FormulaType = 1 AND 
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN DateUsedInLastFetch RP ON RT.FetchDateM = RP.FetchDateM and Backward = 0
	WHERE 
		FormulaType = 2 AND 
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM;
	
	IF @@ROWCOUNT = 0 Return; 
	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FormulaId int, @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	FormulaId  from Formulas	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		EXEC USP_Generate_CalcSQL_ByFormulaRegular @FormulaId, @SQLText OUT;
		EXEC( @SQLText )
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula
	
	--UPDATE CalculationLogs	SET Status=1, Backward=0 WHERE FetchDateM =  1;
	UPDATE A 
	SET Status=1, Backward=0
	FROM CalculationLogs A 
	INNER JOIN (SELECT DISTINCT FetchDateM FROM FormulaRemain) FR ON FR.FetchDateM = A.FetchDateM;
	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas(FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue)
	SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM FormulaRemain;


END;


GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormulaRegular1]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormulaRegular1] AS
BEGIN
	SET NOCOUNT ON;	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------
	--exec dbo.SP_KillProcess;
	TRUNCATE TABLE FormulaRemain;
	TRUNCATE TABLE DateUsedInLastFetch;

	INSERT INTO DateUsedInLastFetch
	SELECT DISTINCT A.FetchDateM, ISNULL(RP.Backward,0) FROM VW_DateUsedInLastFetch A
	INNER JOIN Reports RP ON A.FetchDateM = RP.FetchDateM 

	BEGIN TRY DROP TABLE #VW_DateUsedInLastFetch END TRY BEGIN CATCH END CATCH
	SELECT FetchDateM INTO #VW_DateUsedInLastFetch FROM VW_DateUsedInLastFetch;


	INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL(UsedLedgers,',')) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	INNER JOIN Ledgers L ON L.LedgerId = A.Item 
	WHERE 
		FormulaType = 0 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	WHERE 
		FormulaType = 1 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	WHERE 
		FormulaType = 2 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM;
	
	IF @@ROWCOUNT = 0 Return; 
	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FormulaId int, @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	FormulaId  from Formulas	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		EXEC USP_Generate_CalcSQL_ByFormulaRegular @FormulaId, @SQLText OUT;
		EXEC( @SQLText )
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula
	
	--UPDATE CalculationLogs	SET Status=1, Backward=0 WHERE FetchDateM =  1;
	UPDATE A 
	SET Status=1, Backward=0
	FROM CalculationLogs A 
	INNER JOIN (SELECT DISTINCT FetchDateM FROM FormulaRemain) FR ON FR.FetchDateM = A.FetchDateM;
	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas(FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue)
	SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM FormulaRemain;


END;


GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateAndTransferFormulaRegular2]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateAndTransferFormulaRegular2] AS
BEGIN
	SET NOCOUNT ON;	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------

	BEGIN TRY DROP TABLE #VW_DateUsedInLastFetch END TRY BEGIN CATCH END CATCH
	SELECT FetchDateM INTO #VW_DateUsedInLastFetch FROM VW_DateUsedInLastFetch;

	--exec dbo.SP_KillProcess;
	TRUNCATE TABLE FormulaRemain;
	

	INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL(UsedLedgers,',')) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	INNER JOIN Ledgers L ON L.LedgerId = A.Item 
	WHERE 
		FormulaType = 0 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	WHERE 
		FormulaType = 1 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, RT.FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0
	WHERE 
		FormulaType = 2 AND RT.FetchDateM in( SELECT FetchDateM FROM #VW_DateUsedInLastFetch ) AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, RT.FetchDateM;
	
	IF @@ROWCOUNT = 0 Return; 
	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FormulaId int, @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	FormulaId  from Formulas	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		EXEC USP_Generate_CalcSQL_ByFormulaRegular @FormulaId, @SQLText OUT;
		EXEC( @SQLText )
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula
	
	--UPDATE CalculationLogs	SET Status=1, Backward=0 WHERE FetchDateM =  1;
	UPDATE A 
	SET Status=1, Backward=0
	FROM CalculationLogs A 
	INNER JOIN (SELECT DISTINCT FetchDateM FROM FormulaRemain) FR ON FR.FetchDateM = A.FetchDateM;
	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas(FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue)
	SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM FormulaRemain;


END;


GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateFormulaAlone]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateFormulaAlone](@Code nvarchar(100),@FetchDateM Date) AS
BEGIN
	--DECLARE @Code nvarchar(100),@FetchDateM Date;
	--SELECT @Code ='[F1]',@FetchDateM ='2013-11-09';

	BEGIN TRY	DROP TABLE	#FormulaRemain	END TRY BEGIN CATCH	END CATCH
	CREATE	TABLE #FormulaRemain
	(
		BranchId	int,
		FetchDateM	date,
		Code		nvarchar(100),
		Remain		decimal	(18,4),
		FirstRemain	decimal	(18,4),
		LastRemain	decimal	(18,4),
		CreditValue	decimal	(18,4),
		DebitValue	decimal	(18,4),	
	);
	DECLARE @DrivedTable TABLE 
	(
		FormulaType tinyint, 
		SeqNO int, 
		Code nvarchar(100)
	);	
	INSERT INTO @DrivedTable(FormulaType,SeqNO,Code)
	SELECT FormulaType, SeqNO, Code FROM  dbo.FN_Select_Drived_FormulaToByCode(@Code)
	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------
	INSERT INTO #FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code,	BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL( UsedLedgers,',' )) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 0 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 1 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 2 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM;


	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FCode nvarchar(100), @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	Code  from @DrivedTable	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FCode; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  

		EXEC USP_Generate_CalcSQL_ByFormulaInDate @FCode, @FetchDateM, @SQLText OUT;

		EXEC( @SQLText )
		
		FETCH NEXT FROM Cur_Formula INTO @FCode; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula


	--SELECT * FROM #FormulaRemain

	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM #FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas
	SELECT 	FetchDateM,Code,BranchId,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM #FormulaRemain;

	-- Select * from dbo.FN_Select_Drived_FormulaToByCode('[F1]')
	
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_CalculateFormulaAlone_Discontinued]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CalculateFormulaAlone_Discontinued](@Code nvarchar(100),@FetchDateM Date) AS
BEGIN
	--DECLARE @Code nvarchar(100),@FetchDateM Date;
	--SELECT @Code ='[F1]',@FetchDateM ='2013-11-09';

	BEGIN TRY	DROP TABLE	#FormulaRemain	END TRY BEGIN CATCH	END CATCH
	CREATE	TABLE #FormulaRemain
	(
		BranchId	int,
		FetchDateM	date,
		Code		nvarchar(100),
		Remain		decimal	(18,4),
		FirstRemain	decimal	(18,4),
		LastRemain	decimal	(18,4),
		CreditValue	decimal	(18,4),
		DebitValue	decimal	(18,4),	
	);
	DECLARE @DrivedTable TABLE 
	(
		FormulaType tinyint, 
		SeqNO int, 
		Code nvarchar(100)
	);	
	INSERT INTO @DrivedTable(FormulaType,SeqNO,Code)
	SELECT FormulaType, SeqNO, Code FROM  dbo.FN_Select_Drived_FormulaToByCode(@Code)
	
	---------------------------------------------------------------------------------
	---- First Step: Calculate non manual formulas and insert into FormulaRemain ----
	---------------------------------------------------------------------------------
	INSERT INTO #FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		F.Code,	BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue
	FROM Formulas F
	CROSS APPLY( SELECT Item From dbo.FN_StringToTable_Not_NULL( UsedLedgers,',' )) A
	INNER JOIN ResultTrans RT ON A.Item = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 0 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM 
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId  and L.LedgerCode  Between  F.FromLedger  and F.ToLedger 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 1 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM
	UNION ALL
	SELECT 
		F.Code, BranchId, FetchDateM,
		SUM(Remain) as Remain, 
		SUM(FirstRemain) as FirstRemain, 
		SUM(LastRemain) as LastRemain, 
		SUM(CreditValue) as CreditValue, 
		SUM(DebitValue) as DebitValue 
	FROM Formulas F
	INNER JOIN Ledgers L ON F.GeneralId = L.GeneralId 
	INNER JOIN ResultTrans RT ON L.LedgerId = RT.LedgerId
	INNER JOIN @DrivedTable DF ON DF.Code = '['+F.Code+']'
	WHERE 
		F.FormulaType = 2 AND RT.FetchDateM = @FetchDateM AND
		( LTRIM(RTRIM(BranchesIdList)) = '' OR RT.BranchId in ( SELECT Item From dbo.FN_StringToTable_Not_NULL(BranchesIdList,',')  ))
	GROUP BY F.Code, BranchId, FetchDateM;


	---------------------------------------------------------------------------------
	----- Secoud Step: Calculate manual formulas and append into FormulaRemain ------
	---------------------------------------------------------------------------------
	DECLARE @FCode nvarchar(100), @SQLText nvarchar(MAX)='SELECT 1 as A';
			
	DECLARE Cur_Formula CURSOR	FOR  
		SELECT TOP 10000000	Code  from @DrivedTable	WHERE FormulaType = 3 ORDER BY SeqNO
				
	OPEN Cur_Formula  
	FETCH NEXT FROM Cur_Formula INTO @FCode; 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  

		EXEC USP_Generate_CalcSQL_ByFormulaInDate @FCode, @FetchDateM, @SQLText OUT;

		EXEC( @SQLText )
		
		FETCH NEXT FROM Cur_Formula INTO @FCode; 
	END  

	CLOSE Cur_Formula  
	DEALLOCATE Cur_Formula


	--SELECT * FROM #FormulaRemain

	---------------------------------------------------------------------------------
	----- Third Step: Transfer Calculated Formulas to the ResultFormulas  -----------
	---------------------------------------------------------------------------------
	DELETE ResultFormulas 
	FROM #FormulaRemain FR
	WHERE 
		FR.BranchId = ResultFormulas.BranchId AND 
		FR.FetchDateM = ResultFormulas.FetchDateM AND 
		FR.Code = ResultFormulas.Code;

	INSERT INTO ResultFormulas
	SELECT 	BranchId,FetchDateM,Code,Remain,FirstRemain,LastRemain,CreditValue,DebitValue
	FROM #FormulaRemain;

	-- Select * from dbo.FN_Select_Drived_FormulaToByCode('[F1]')
	
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Check_Access_Level]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Check_Access_Level]( @UserId int, @AccessCode Int, @OwnerUserId Int)  AS
BEGIN
	DECLARE @RET Int= 0, @AdminCanModify tinyint=0;
	DECLARE @UserGroupId Int,@AccessMode Int, @OwnerUserGroupId Int,@OwnerAccessMode Int, @ForceInGroup tinyint;

	SELECT @UserGroupId = UserGroupId, @AccessMode = AccessMode FROM Users WHERE userId = @UserId
	SELECT @OwnerUserGroupId = UserGroupId, @OwnerAccessMode = AccessMode FROM Users WHERE userId = @OwnerUserId
	SELECT @ForceInGroup = ForceInGroup from Accesses WHERE AccessCode = @AccessCode
	SELECT @AdminCanModify=ISNULL(AdminCanModify,0) From Configs;
	
	-- اگر رکورد جاري متعلف به خود کاربر باشد مجوز دارد
	--همچنین اگر لیست دسترسی هنوز اجرایی نشده باشد دسترسی برا ی همه آزاد است
	IF ( @UserId = @OwnerUserId ) OR ( NOT EXISTS( Select 1 from Accesses WHERE AccessCode = @AccessCode )) 
		SET @RET = 1 
	ELSE
	IF EXISTS( 	SELECT 1 FROM Users WHERE UserId = @UserId and IsAdmin =1 and @AdminCanModify =1) 
		SET @RET = 1  -- کاربر ادمين است
	ELSE
	BEGIN
		IF EXISTS(
					SELECT 1 FROM Accesses AC
					LEFT JOIN UserAccesses UA ON AC.AccessId = UA.AccessId
					WHERE UserId = @UserId AND AC.AccessCode = @AccessCode
				) 
		BEGIN
			IF (@OwnerUserId <> -1 ) AND ( @ForceInGroup =1 ) 
			BEGIN
				IF ( @UserGroupId=@OwnerUserGroupId ) AND ( @AccessMode < @OwnerAccessMode )-- در سطح بالاتري است
					SET @RET =1	
			END
			ELSE
			SET @RET =1
		END	

	END
	
	SELECT @Ret as RET
--1085  -> Show All Logs
--1090  -> Scheduleing
--1095  -> DetailFormula
END;










GO

/****** Object:  StoredProcedure [dbo].[USP_CheckCircularFormula]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CheckCircularFormula] AS
BEGIN
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			SeqNo, Code, '['+Code+']' as Code2, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	 (
		SELECT 
			SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY( SELECT Item FROM dbo.FN_StringToTable_Not_NULL( B.UsedLedgersAbr2,',') ) A 
	)
	SELECT SeqNo,Code,Code2,Lst FROM CTE_ALL A
	WHERE Lst IN ( SELECT Code2 FROM CTE_ALL WHERE SeqNO >= A.SeqNO);

END









GO

/****** Object:  StoredProcedure [dbo].[USP_CopyDataToResultTrans]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_CopyDataToResultTrans] AS
BEGIN
	DELETE DayResultTrans 
	WHERE BranchId NOT IN ( SELECT BranchId FROM Branches);  	

	DELETE	ResultTrans
	FROM VW_ResultTrans_FULL T1
	INNER JOIN DayResultTrans T2 
							ON  T1.BranchId		= T2.BranchId AND 
								T1.GeneralCode	= T2.GeneralCode AND 
								T1.LedgerCode	= T2.LedgerCode AND
								T1.SystemId		= T2.SystemId AND
								T1.FetchDateM	= T2.FetchDateM; 
	;WITH CTE_DayResultTrans AS
	(
		SELECT  
			GLC.LedgerId, TRT.BranchId, TRT.FetchDateM, DT.Shamsi as FetchDateS, TRT.Remain, TRT.FirstRemain, TRT.LastRemain, TRT.CreditValue, TRT.DebitValue
		FROM DayResultTrans TRT 
		INNER JOIN VW_GeneralLedgerCoding GLC 
				ON GLC.LedgerCode = TRT.LedgerCode and TRT.GeneralCode = GLC.GeneralCode and TRT.SystemId = GLC.SystemId
		LEFT JOIN TblDate DT ON TRT.FetchDateM = DT.MiladiDate
	)
	
	INSERT INTO ResultTrans(LedgerId, BranchId, FetchDateM, FetchDateS, Remain, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT 
		LedgerId, BranchId, FetchDateM, FetchDateS,  Remain, FirstRemain, LastRemain, CreditValue, DebitValue 
	FROM  CTE_DayResultTrans
	WHERE BranchId IN ( SELECT  BranchId FROM Branches)
	UNION
	SELECT LedgerId, BranchId, FetchDateM,FetchDateS, LastRemain, FirstRemain, LastRemain, CreditValue, DebitValue   	
	FROM OtherBarnchesData
END










GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_BranchMergeInfo]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Delete_BranchMergeInfo]( @BranchMergeInfoId int ) AS
BEGIN
	DELETE BranchMergeInfo
	WHERE BranchMergeInfoId= @BranchMergeInfoId;
END;











GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_Formulas]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_Delete_Formulas](@FormulaId int) AS
BEGIN

	DECLARE @Ret int=0,@RetFormula nvarchar(4000);
	SELECT @RetFormula= dbo.FN_CanDeleteFormula( @FormulaId );
	IF @RetFormula = '' 
		DELETE Formulas	WHERE FormulaId= @FormulaId;
	ELSE
		SET @Ret =-2
	RETURN @Ret;			
END










GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_ImportExcelSchema]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Delete_ImportExcelSchema]( @ImportSchemaId int ) AS
BEGIN

	DELETE   ImportSchemas
	WHERE ImportSchemaId= @ImportSchemaId

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_ImportSchedule]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Delete_ImportSchedule]( @ImportScheduleId int ) AS
BEGIN
	IF EXISTS( SELECT 1 FROM ImportSchedule WHERE ImportScheduleId= @ImportScheduleId and ImportStatus <> 0 )
		RETURN -1
	ELSE
		DELETE ImportSchedule
		WHERE ImportScheduleId= @ImportScheduleId;
		RETURN 0;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_UserGroups]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Delete_UserGroups]( @GroupName as nvarchar(50) ) AS
BEGIN
	BEGIN TRY
		DELETE UserGroups WHERE GroupName = @GroupName
		RETURN 0
	END TRY
	BEGIN CATCH
		RETURN -1
	END CATCH
END;








GO

/****** Object:  StoredProcedure [dbo].[USP_Delete_Users]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Delete_Users]( @UserId int ) AS
BEGIN
	BEGIN TRY
		DELETE	Users	WHERE UserId = @UserId
		RETURN 0
	END TRY
	BEGIN CATCH
		RETURN -1
	END CATCH
END;








GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormula]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormula]( @FormulaId int, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE FormulaId= @FormulaId

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') ';
	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM, '+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''RM'' as Node,Code as Lst,BranchId,FetchDateM,Remain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''RM'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, Remain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+ 
	'	PIVOT ( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,RM,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([RM],[FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END









GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormula_Runtime]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormula_Runtime]( @FormulaId int, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';
			
	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE FormulaId= @FormulaId

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') ';


IF CHARINDEX ('/', @Context) >0 
BEGIN
SET 
@RetSQL=' 
	SET ANSI_WARNINGS OFF;
	SET ARITHABORT OFF;
	'
END
ELSE 
BEGIN
SET 
@RetSQL=' 
	SET ANSI_WARNINGS ON;
	SET ARITHABORT ON;
	'
END;
	
	SET @RetSQL= @RetSQL+CHAR(13)+
	'IF EXISTS( Select top 1 * from ResultFormulas Where Code = '''+@Code+''' and IsPermanent = 0 )
	DELETE  ResultFormulas Where Code ='''+@Code+''''+CHAR(13)+
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM ResultFormulas '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM ResultFormulas '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM ResultFormulas '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM ResultFormulas '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO ResultFormulas(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue, IsPermanent) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV,0 FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END

GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaBackward]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaBackward]( @FormulaId int, @FetchDateM Date, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , 
			@BranchesIdList nvarchar(MAX)='', 
			@Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM FormulasBackward WHERE FormulaId= @FormulaId

	SET @Where= '		WHERE FetchDateM ='''+CAST(@FetchDateM as varchar(10) )+''' ';
	IF @BranchesIdList<>'' 
			SET @Where= @Where+ ' AND BranchId in('+@BranchesIdList+') ';

	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', LedgerTotal,BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', LedgerTotal,BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', LedgerTotal,BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', LedgerTotal,BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SELECT @CalSQLStatement = @RetSQL

END




GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaBackward0]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaBackward0]( @FormulaId int, @FetchDateM Date, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , 
			@BranchesIdList nvarchar(MAX)='', 
			@Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM FormulasBackward WHERE FormulaId= @FormulaId

	SET @Where= '		WHERE FetchDateM ='''+CAST(@FetchDateM as varchar(10) )+''' ';
	IF @BranchesIdList<>'' 
			SET @Where= @Where+ ' AND BranchId in('+@BranchesIdList+') ';

	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SELECT @CalSQLStatement = @RetSQL

END




GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaInDate]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaInDate]( @Code nvarchar(100), @FetchDate Date, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE '['+Code+']'= @Code

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') '
	ELSE
			SET @Where= '		WHERE (1=1) ';
	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM, '+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''RM'' as Node,Code as Lst,BranchId,FetchDateM,Remain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''RM'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, Remain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+ 
	'	PIVOT ( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO #FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,RM,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([RM],[FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END









GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaInDate_Discontinued]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaInDate_Discontinued]( @Code nvarchar(100), @FetchDate Date, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE '['+Code+']'= @Code

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') '
	ELSE
			SET @Where= '		WHERE (1=1) ';
	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM, '+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''RM'' as Node,Code as Lst,BranchId,FetchDateM,Remain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''RM'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, Remain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+ 
	'	PIVOT ( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM #FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+@Where+CHAR(13)+
	'			AND	RT.FetchDateM = '''+CAST(@FetchDate as varchar(10))+''' '+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO #FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,RM,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([RM],[FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END










GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaRegular]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaRegular]( @FormulaId int, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE FormulaId= @FormulaId

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') ';
	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', LedgerTotal,BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', LedgerTotal,BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', LedgerTotal,BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', LedgerTotal,BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END


GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaRegular0]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaRegular0]( @FormulaId int, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE FormulaId= @FormulaId
	
	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') ';
	SET @RetSQL=
	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM AND Backward = 0  '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END


GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CalcSQL_ByFormulaRegular2]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_Generate_CalcSQL_ByFormulaRegular2]( @FormulaId int, @CalSQLStatement nvarchar(MAX) OUT ) AS
BEGIN
	DECLARE @RetSQL nvarchar(MAX)='', @Code nvarchar(100)='', @Context nvarchar(Max)='', 
			@UsedLedgersAbr nvarchar(MAX)='' , @BranchesIdList nvarchar(MAX)='', @Where nvarchar(MAX)='';

	SELECT 
		@Code=Code, 
		@Context= Replace( Replace( Context ,'[','ISNULL('),']',',0)') , 
		@UsedLedgersAbr=UsedLedgersAbr ,
		@BranchesIdList = LTRIM(RTRIM(ISNULL(BranchesIdList,'')))
	FROM Formulas WHERE FormulaId= @FormulaId

	IF @BranchesIdList<>'' 
			SET @Where= '		WHERE BranchId in('+@BranchesIdList+') ';
	SET @RetSQL=
	'	DECLARE @VW_DateUsedInLastFetch as TABLE ( FetchDateM DATE ); '+CHAR(13)+
	'	INSERT  @VW_DateUsedInLastFetch SELECT FetchDateM FROM VW_DateUsedInLastFetch; '+CHAR(13)+

	';WITH CTE_Nodes AS '+CHAR(13)+
	'( '+CHAR(13)+
	'SELECT Node,BranchId,FetchDateM, '+@Context+' as Value '+CHAR(13)+
	'FROM( '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''FR'' as Node,Code as Lst,BranchId,FetchDateM,FirstRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''FR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, FirstRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN @VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+
	'		INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0 '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''LR'' as Node,Code as Lst,BranchId,FetchDateM,LastRemain as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''LR'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, LastRemain '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN @VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+
	'		INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0 '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''CV'' as Node,Code as Lst,BranchId,FetchDateM,CreditValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''CV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, CreditValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN @VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+
	'		INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0 '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ( '+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	'	UNION ALL '+CHAR(13)+
	'	SELECT Node,BranchId,FetchDateM,'+@UsedLedgersAbr+' '+CHAR(13)+
	'	FROM '+CHAR(13)+
	'	( '+CHAR(13)+
	'		SELECT ''DV'' as Node,Code as Lst,BranchId,FetchDateM,DebitValue as RetCalc '+CHAR(13)+
	'		FROM FormulaRemain '+CHAR(13)+@Where+CHAR(13)+
	'		UNION '+CHAR(13)+
	'		SELECT  ''DV'', ''[''+LedgerTotal+'']'',BranchId, Rt.FetchDateM, DebitValue '+CHAR(13)+
	'		FROM ResultTrans RT '+CHAR(13)+
	'		INNER JOIN Ledgers L ON L.LedgerId = RT.LedgerId '+CHAR(13)+
	'		INNER JOIN @VW_DateUsedInLastFetch DT ON Dt.FetchDateM = RT.FetchDateM '+CHAR(13)+
	'		INNER JOIN Reports	   RP ON RT.FetchDateM = RP.FetchDateM and ISNULL(Backward,0) = 0 '+CHAR(13)+@Where+CHAR(13)+
	'	) Tbl '+CHAR(13)+
	'	PIVOT( SUM(RetCalc) FOR Lst IN ('+@UsedLedgersAbr+') ) Pvt1 '+CHAR(13)+
	') RET '+CHAR(13)+
	') '+CHAR(13)+
	'INSERT INTO FormulaRemain(Code, BranchId, FetchDateM, Remain, FirstRemain, LastRemain, CreditValue, DebitValue) '+CHAR(13)+
	'Select '''+@Code+''' as Code,BranchId,FetchDateM,LR,FR,LR,CV,DV FROM '+CHAR(13)+
	'( '+CHAR(13)+
	' SELECT Node,BranchId,FetchDateM,Value FROM CTE_Nodes '+CHAR(13)+
	' )TBL '+CHAR(13)+
	'PIVOT( SUM(Value) FOR Node IN([FR],[LR],[CV],[DV])) Pvt '+CHAR(13)
	;
	SET @CalSQLStatement = @RetSQL

END


GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CompressedFormulaForALL]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CompressedFormulaForALL] AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY	DROP TABLE #TempFormula	END TRY BEGIN CATCH END CATCH;
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			SeqNo, Code, '['+Code+']' as Code2, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	 (
		SELECT 
			SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY( SELECT Item FROM dbo.FN_StringToTable_Not_NULL( B.UsedLedgersAbr2,',') ) A 
	)
	SELECT SeqNo,Code,Code2,Lst INTO #TempFormula 
	FROM CTE_ALL A
	WHERE LST IN ( SELECT CODE2 FROM CTE_ALL);

	BEGIN TRANSACTION	
		UPDATE 	Formulas	SET CompressedFormula = Context;
		DECLARE 
			@SeqNo int =0, 
			@Code nvarchar(100)='', 
			@LST nvarchar(100)='', 
			@Context nvarchar(max)='';
		
		DECLARE Cur_Formula CURSOR	FOR  
				SELECT F.SeqNo, F.Code, T.LST 
				FROM Formulas F
				INNER JOIN #TempFormula T ON '['+F.Code+']' = T.Code2;
				
		OPEN Cur_Formula  
		FETCH NEXT FROM Cur_Formula INTO @SeqNo, @Code, @LST 

		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			SELECT @Context = '('+CompressedFormula+')'  FROM Formulas WHERE '['+Code+']' = @LST ;
			UPDATE Formulas SET CompressedFormula = REPLACE(CompressedFormula, @LST, @Context) WHERE Code = @Code;
			FETCH NEXT FROM Cur_Formula INTO @SeqNo, @Code, @LST 
		END  

		CLOSE Cur_Formula  
		DEALLOCATE Cur_Formula
	COMMIT
	SET NOCOUNT OFF;
	
END;










GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_CompressedFormulaForById]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_CompressedFormulaForById]( @FormulaId int, @RunFormula tinyint, @ErrTitle nvarchar(4000) OUT, @CompressedFormulaValue nvarchar(MAX) OUT ) AS
--Declare @FormulaId int= '103390', @RunFormula tinyint=1, @ErrTitle nvarchar(4000) , @CompressedFormulaValue nvarchar(MAX);
BEGIN
	SET NOCOUNT ON;
	SET @ErrTitle= '0.0';
    SET @CompressedFormulaValue='';

	BEGIN TRY	DROP TABLE #Temp_Formula		END TRY BEGIN CATCH END CATCH;
	BEGIN TRY	DROP TABLE #Formulas	END TRY BEGIN CATCH END CATCH;
	-- Start of CTE
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			SeqNo, Code, '['+Code+']' as Code2, 
			'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	 (
		SELECT 
			SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY( SELECT Item FROM dbo.FN_StringToTable_Not_NULL( B.UsedLedgersAbr,',') ) A 
	)
	SELECT SeqNo,Code,Code2,Lst 
	INTO #Temp_Formula 
	FROM CTE_ALL A
	WHERE LST IN ( SELECT CODE2 FROM CTE_ALL);
	-- END Of CTE
	
	DECLARE @CompressedFormula nvarchar(max)='';

	SELECT * INTO #Formulas FROM Formulas;

	BEGIN TRANSACTION	
		UPDATE 	#Formulas	SET CompressedFormula = Context;

		DECLARE 
			@SeqNo int =0, 
			@Code nvarchar(100)='', 
			@LST nvarchar(100)='', 
			@Context nvarchar(max)='';
		
		DECLARE Cur_Formula CURSOR	FOR  
				SELECT F.SeqNo, F.Code, T.LST 
				FROM #Formulas F
				INNER JOIN #Temp_Formula T ON '['+F.Code+']' = T.Code2;
				
		OPEN Cur_Formula  
		FETCH NEXT FROM Cur_Formula INTO @SeqNo, @Code, @LST 

		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			SELECT @Context = '('+CompressedFormula+')'  FROM #Formulas WHERE '['+Code+']' = @LST ;
			UPDATE #Formulas SET CompressedFormula = REPLACE(CompressedFormula, @LST, @Context) WHERE Code = @Code;
			FETCH NEXT FROM Cur_Formula INTO @SeqNo, @Code, @LST 
		END  

		CLOSE Cur_Formula  
		DEALLOCATE Cur_Formula
	COMMIT

	SELECT @CompressedFormula = CompressedFormula FROM #Formulas
	WHERE FormulaId = @FormulaId;
	
	SET @CompressedFormulaValue= @CompressedFormula
	IF @RunFormula=1
	BEGIN
		BEGIN TRY	DROP TABLE #TEST		END TRY BEGIN CATCH END CATCH;
		CREATE	TABLE #TEST( 
					LedgerAbrList nvarchar(max), LedgerCodeList nvarchar(max), LedgerIdList nvarchar(max), 
					Formula nvarchar(max), LedgerTitleList nvarchar(max), ErrorList nvarchar(4000), HasError int);
		DECLARE 
			@LedgerAbrList nvarchar(max)='', 
			@LedgerAbrListISNULL nvarchar(max)='', 
			@Formula nvarchar(max)='',
			@SQLCode nvarchar(max)=''; 

		INSERT INTO #TEST	EXEC USP_TestValidityFormula @CompressedFormula
			SELECT 
				@LedgerAbrList = LedgerAbrList,
				@LedgerAbrListISNULL = 'ISNULL('+REPLACE(LedgerAbrList, ',',',0),ISNULL(')+',0)' , 
				@Formula= Formula 
			FROM #TEST;
		BEGIN TRY	DROP TABLE #TEST	END TRY BEGIN CATCH END CATCH;
	
	
		SET @SQLCode =
			'SELECT SUM('+@Formula+') as Title '+CHAR(13)+'FROM'+CHAR(13)+
			'('+CHAR(13)+
			'	SELECT '+@LedgerAbrListISNULL+CHAR(13)+
			'	FROM ( SELECT TOP 1 BranchId, LedgerTotal, Remain FROM VW_ResultTrans_FULL ) as TBL '+CHAR(13)+
			'	PIVOT('+CHAR(13)+
			'		SUM(Remain) FOR LedgerTotal IN'+CHAR(13)+
			'									('+@LedgerAbrList+')'+CHAR(13)+
			'		) PVT'+CHAR(13)+
			')'+CHAR(13)+
			'K('+@LedgerAbrList+')';
	
		DECLARE @OUT nvarchar(4000)='';
		BEGIN TRY
			EXECUTE	USP_RunPivotTest @SQLCode, @OUT output
			SET @ErrTitle = ISNULL( NULLIF(LTRIM(RTRIM(@OUT)),'') ,1);
			SELECT ISNULL( NULLIF(LTRIM(RTRIM(@OUT)),'') ,1) as Title
		END TRY
		BEGIN CATCH
			SET @ErrTitle = ERROR_MESSAGE();
			SELECT ERROR_MESSAGE() as Title
		END CATCH
	END
END;


GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_Simple_Formula]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_Simple_Formula]( @GeneralId int, @FromLedgers nvarchar(max),@ToLedgers nvarchar(max), @Mode tinyint) AS
BEGIN
	-- @Mode = 0  -> Include
	-- @Mode = 1  -> From TO
	-- @Mode = 2  -> For General
	-- @Mode = 3  -> Manual
	--Declare @GeneralId int= 304, @FromLedgers nvarchar(max)='10,11,12,13,14,15,16,17,18',@ToLedgers nvarchar(max)='', @Mode tinyint=0 ;
	
	DECLARE 
		@LedgerCode    nvarchar(Max)='', 
		@LedgerId      nvarchar(Max)='',
		@LedgerAbr     nvarchar(Max)='',
		@LedgerAbrPlus nvarchar(Max)='',
		@LedgerTotal   nvarchar(Max)='';

	DECLARE @RemoveComments TinyInt=0;
	SELECT TOP 1  @RemoveComments=RemoveComments FROM Configs;

	IF @Mode = 0
		SELECT 
			@LedgerTotal = Concat( @LedgerTotal,'[',LedgerTotal,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerCode = Concat( @LedgerCode,'[',LedgerCode,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerId = Concat(@LedgerId, Case @LedgerId when '' then '' else ',' end ,LedgerId)
			,
			@LedgerAbr = Concat(@LedgerAbr, Case @LedgerAbr when '' then '' else ',' end ,LedgerTotal)
			,
			@LedgerAbrPlus = Concat(@LedgerAbrPlus, Case @LedgerAbrPlus when '' then '[' else '+[' end ,LedgerTotal,']')

		FROM VW_GeneralActiveLedgers
		WHERE LedgerId in( Select Item FROM  dbo.FN_StringToTable_Not_NULL(@FromLedgers,',')  )
	ELSE
	IF @Mode in (1,2)
		SELECT 
			@LedgerTotal = Concat( @LedgerTotal,'[',LedgerTotal,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerCode = Concat( @LedgerCode,'[',LedgerCode,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerId = Concat(@LedgerId, Case @LedgerId when '' then '' else ',' end ,LedgerId)
			,
			@LedgerAbr = Concat(@LedgerAbr, Case @LedgerAbr when '' then '' else ',' end ,LedgerTotal)
			,
			@LedgerAbrPlus = Concat(@LedgerAbrPlus, Case @LedgerAbrPlus when '' then '[' else '+[' end ,LedgerTotal,']')

		FROM VW_GeneralActiveLedgers
		WHERE GeneralId = @GeneralId AND ( @Mode =2 OR LedgerCode BETWEEN  @FromLedgers AND @ToLedgers)

	SELECT  
			LTRIM(RTRIM(@LedgerCode)) as LedgerCodeList, 
			LTRIM(RTRIM(@LedgerId)) as LedgerIdList, 
			LTRIM(RTRIM(@LedgerAbr)) as LedgerAbrList, 
			LTRIM(RTRIM(@LedgerAbrPlus)) as LedgerAbrPlusList,
			LTRIM(RTRIM(@LedgerTotal)) as LedgerTotalList 

END;










GO

/****** Object:  StoredProcedure [dbo].[USP_Generate_Simple_FormulaForBranches]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Generate_Simple_FormulaForBranches]( @BranchesId nvarchar(max)) AS
--Declare @BranchesId nvarchar(max)='66666,11111,140,144,1243,1258,1328,1368,493,494';
BEGIN

	DECLARE 
		@BranchName    nvarchar(Max)='', 
		@BranchId      nvarchar(Max)='',
		@BranchTotal   nvarchar(Max)='';

		SELECT
		
			@BranchTotal = Concat( @BranchTotal,'[',BranchId,'] /*',BranchName,'*/',Char(13))
			,
			@BranchId = Concat(@BranchId, Case @BranchId when '' then '' else ',' end ,BranchId)
			,
			@BranchName = Concat(@BranchName, Case @BranchName when '' then '' else ',' end ,BranchName)
			
		FROM VW_BranchesAll
		WHERE BranchId in( Select Item FROM  dbo.FN_StringToTable_Not_NULL(@BranchesId,',')  );

		SELECT  
			LTRIM(RTRIM(@BranchName))  as BranchNameList,  
			LTRIM(RTRIM(@BranchId))    as BranchIdList, 
			LTRIM(RTRIM(@BranchTotal)) as BranchTotalList 

END;










GO

/****** Object:  StoredProcedure [dbo].[USP_Get_NewSeqNO]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Get_NewSeqNO] AS
	SELECT ISNULL(MAX(SeqNO),0)+1 as NewSeqNO FROM Formulas










GO

/****** Object:  StoredProcedure [dbo].[USP_ImportData]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC  [dbo].[USP_ImportData] AS
	/*	
		0: Run USP_ReconfigReportsForFetch in Job list;
		1: USP_ImportData	
				Truncate DayResultTrans;
				Fill DayResultTrans by USP_ImportDataByDateFromServer; 
				Copy Data to ResultTrans by USP_CopyDataToResultTrans;
		2: USP_CalculateAndTransferFormula {Regular , Backward}
				Truncate FormulaRemain;
				Fill FormulaRemain for formulaTypes(0,1,2);
				Calculate manual formulas by USP_Generate_CalcSQL_ByFormula;
				Fill ResultFormulas from FormulaRemain
	*/
	SET NOCOUNT ON;
	DECLARE @FetchDate as Date = Getdate()-1, @MaxDateCount Int ;
	DECLARE @ActiveLedgers AS ActiveLedgersType;
	
	BEGIN TRY DROP TABLE #TEMPDATE			END TRY BEGIN CATCH END CATCH;

	TRUNCATE TABLE DayResultTrans;
	-- داده هايي که در تاريخ مشخص محاسبه و آورده نشده اند شناسايي مي شوند
	-- شامل تاريخ هائيست که بايد آورده شوند #TEMPDATE
	SELECT ROW_NUMBER() OVER( ORDER BY DT.MiladiDate) as RW, DT.MiladiDate, DT.Shamsi 
	INTO #TEMPDATE
	FROM TblDate DT
	LEFT JOIN Reports RP ON DT.MiladiDate = RP.FetchDateM
	WHERE 
		( DT.MiladiDate BETWEEN ( SELECT TOP 1 StartFetchDateM FROM Configs ) AND @FetchDate ) AND 
		( RP.FetchDateM IS NULL OR ISNULL(RP.Retry,0) =1 OR status =0 )	OR	
		( Status = 0 and Backward = 1)
	SELECT @MaxDateCount = Count(*) FROM #TEMPDATE;
	
	-- ليست کدهاي کل و معين هائي که بايد آورده شوند را بصورت جدول مرتب مي کند
	-- به ازاي هر کل ، معين هاي مرتبط با کاما به هم وصل ميشوند
	-- ROWNO	GeneralId	LedgerList
	-- 1		1030		2,3,4,5
	-- 2		1060		1
	-- 3		1120		1,94

	;WITH GeneralActiveLedgers AS
	(
		SELECT L.LedgerCode, G.GeneralCode, S.SystemId
		FROM ActiveLedgers A
		INNER JOIN Ledgers L  ON A.LedgerId = L.LedgerId
		INNER JOIN Generals G ON L.GeneralId = G.GeneralId 
		LEFT  JOIN Systems  S ON S.SystemId  = G.SystemId
	)
	INSERT INTO @ActiveLedgers
	SELECT ROW_NUMBER() OVER( ORDER BY SystemId, GeneralCode) as RowNO, SystemId, GeneralCode,CONVERT(varchar(8000) ,LedgersList) LedgersList 
	FROM
	(
		SELECT DISTINCT
				A.GeneralCode, A.SystemId,
				CAST( (REPLACE(','+B.LedgersList,',,',''))  as varchar(8000) ) as LedgersList 
		FROM GeneralActiveLedgers A
		CROSS APPLY ( 
						SELECT Concat(',', LedgerCode ) FROM GeneralActiveLedgers 
						WHERE SystemId = A.SystemId AND GeneralCode = A.GeneralCode AND LedgerCode IS NOT NULL
						FOR XML PATH('')  
					) B(LedgersList)
	) T

	DECLARE @Date Date , @MaxCount Int, @I Int = 1, @ErrCode Int =0;
	SELECT @MaxCount = Max(RowNO) FROM @ActiveLedgers;

	-- به ازاي هر روز USP_ImportDataByDateFromServer به ازاي هر کل ، معين هاي شناسايي شده از طريق پروسه 
	-- با ستونهاي زير ذخيره ميشوند  DayResultTrans صدا زده ميشوند و نتيجه آن در جدول 
	--	Column_name	Type
	--	ShamsiInt	int
	--	BranchId	int
	--	GeneralCode	int
	--	LedgerCode	int
	--	FetchDateM	date
	--	Remain		decimal
	--	LastRemain	decimal
	--	DebitValue	decimal
	--	CreditValue	decimal
	--	FirstRemain	decimal

	WHILE @I<= @MaxDateCount
	BEGIN
		SELECT @Date = MiladiDate  FROM #TEMPDATE WHERE RW = @I;
		BEGIN TRY

			EXEC USP_ImportDataByDateFromServer @Date, @MaxCount, @ActiveLedgers ;
			EXEC Update_KhadamatNovin_Remain @Date;
			exec SP_Update_Merged_Branches @Date;
			
			DELETE DayResultTrans WHERE BranchId=0;
			
			IF NOT EXISTS( SELECT 1 FROM Reports WHERE FetchDateM= @Date)
				INSERT Reports VALUES( GETDATE(), dbo.FN_MiladiToShamsi(GETDATE()), @Date, dbo.FN_MiladiToShamsi(@Date) ,1,NULL, 0, 0 )
			ELSE 
				UPDATE Reports 
				SET 
					ActionDateM = GETDATE(),
					ActionDateS = dbo.FN_MiladiToShamsi(GETDATE()),
					FetchDateM  = @Date,
					FetchDateS = dbo.FN_MiladiToShamsi(@Date),
					Status= 1,
					MsgError = NULL, 
					ReTry=0 
				WHERE FetchDateM  = @Date
		END TRY
		BEGIN CATCH

			IF NOT EXISTS( SELECT 1 FROM Reports WHERE FetchDateM= @Date)
				INSERT Reports VALUES( GETDATE(), dbo.FN_MiladiToShamsi(GETDATE()), @Date, dbo.FN_MiladiToShamsi(@Date) , -1, ERROR_MESSAGE(), 1, 0 )
			ELSE 
				UPDATE Reports 
				SET 
					ActionDateM =GETDATE(),
					FetchDateM= @Date,
					FetchDateS = dbo.FN_MiladiToShamsi(@Date),
					Status= -1,
					MsgError = ERROR_MESSAGE(), 
					ReTry=1 
				WHERE FetchDateM  = @Date
			SET @ErrCode = -1;
			BREAK

		END CATCH
		SET @I = @I+1
	END

	-- همسان سازي فيلد ها
	IF @ErrCode <> 0 
		RETURN;
	UPDATE DayResultTrans
		SET 
			Remain = LastRemain,
			FetchDateM = D.MiladiDate
	FROM TblDate D
	WHERE D.ShamsiInt = DayResultTrans.ShamsiInt;

	-- ذخيره شود ResultTrans نتيجه بدست آمده بايد در جدول 
	-- درج ميشوند ResultTrans در اينجا اضافات حذف و فقط داده هاي معتبر در جدول 
	
	
	EXEC USP_CopyDataToResultTrans;

	SET NOCOUNT OFF;



GO

/****** Object:  StoredProcedure [dbo].[USP_ImportDataByDateFromServer]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC	[dbo].[USP_ImportDataByDateFromServer]( @Date Date, @Count Int ,  @LedgersCodeList ActiveLedgersType READONLY) AS
BEGIN
	DECLARE @SystemId int, @Dt int, @I Int=1, @Cnt Int=0, @GeneralCode Int, @LedgersList varchar(4000);

	SELECT  @Dt = ShamsiInt  From TblDate Where MiladiDate = CONVERT(Date,@Date);

	WHILE @I<= @Count
	BEGIN
		
		SELECT	@SystemId = SystemId, @GeneralCode = GeneralId, @LedgersList = LedgersList  
		FROM @LedgersCodeList 
		WHERE RowNO = @I;
		 
		BEGIN TRY 
			if (@SystemId=2)
				begin 
						INSERT INTO DayResultTrans
							( ShamsiInt, BranchId, GeneralCode, SystemId, LedgerCode,  LastRemain, DebitValue, CreditValue, FirstRemain)
							--EXEC   [REFAHSERVER].[Taraz].[dbo].[spGetDataByDate_ForBSC]   @Dt, /*@SystemId,*/ @GeneralCode, @LedgersList,'0'
							EXEC [REFAHSERVER].[Taraz92].[dbo].[spGetDataByDate_ForBSC]   @Dt, /*@SystemId,*/ @GeneralCode, @LedgersList,'0'
							
							
							
							 
				end 
			if (@SystemId=3 or @SystemId=4)
				begin 
				INSERT INTO DayResultTrans
							( ShamsiInt, BranchId, GeneralCode, SystemId, LedgerCode,  LastRemain, DebitValue, CreditValue, FirstRemain)
							EXEC [10.16.0.11\SQL2008R2].[CashDB].[dbo].[spGetDataByDate_ForBSC]   @Dt, @SystemId, @GeneralCode, @LedgersList,'0'
				end
		END TRY
		BEGIN CATCH
			THROW;
		END CATCH
		SET @I = @I+1
	END

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_ImportDataFromExcel]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ImportDataFromExcel](@FileNamePath nvarchar(4000), @SheetName nvarchar(100), 
@ActionDate char(10), @LedgerId int ) AS
BEGIN
	--Declare @SheetName nvarchar(100)='Sheet1$',@FileNamePath nvarchar(4000)= '\\192.168.1.5\BSCFolder\Excel-2-4070-9\921205.xlsx';
	--Declare @ActionDate nvarchar(10)='1392/12/24', @LedgerId int=1644;
	Declare @FetchDateM Date;
	
	SELECT @FetchDateM=MiladiDate From TblDate WHERE Shamsi= @ActionDate;

	BEGIN TRY DROP TABLE #TEMP_ExcelData END TRY BEGIN CATCH END CATCH; 
	
	CREATE	TABLE #TEMP_ExcelData( 
								BranchId nvarchar(1000), 
								FirstRemain nvarchar(1000), 
								LastRemain nvarchar(1000), 
								CreditValue nvarchar(1000), 
								DebitValue nvarchar(1000)
								)
	
	INSERT INTO #TEMP_ExcelData
	EXEC(
	'
		SELECT * 
		FROM 
		OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',''Excel 12.0;HDR=YES;'+
					'Database='+@FileNamePath+';'',''Select * FROM ['+@SheetName+'A:E]'') 

	'
	);
	
	DELETE #TEMP_ExcelData 
	WHERE BranchId IS NULL OR FirstRemain IS NULL ;

	DELETE  OtherBarnchesData 
	WHERE 
		LedgerId = @LedgerId and 
		FetchDateM= @FetchDateM  and
		BranchId in( SELECT BranchId FROM #TEMP_ExcelData );


	INSERT INTO OtherBarnchesData(LedgerId, BranchId, FetchDateM,FetchDateS, FirstRemain, LastRemain, CreditValue, DebitValue )
	SELECT @LedgerId,BranchId, @FetchDateM, @ActionDate, FirstRemain, LastRemain,CreditValue, DebitValue FROM #TEMP_ExcelData;
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_ActiceLedgers_ByList]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_ActiceLedgers_ByList]( @LedgerList nvarchar(max)) AS
BEGIN
	INSERT INTO ActiveLedgers(LedgerId,Status)	
	SELECT Item as LedgerId,1
	FROM dbo.FN_StringToTable_Not_NULL(@LedgerList,',')  
	EXCEPT
	SELECT LedgerId,Status FROM ActiveLedgers
	RETURN @@ROWCOUNT;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_BranchMergeInfo]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_BranchMergeInfo]( @BranchCodeFrom int,@BranchCodeTo int,@MergeDateM Date,@ReopenDateM Date) AS
BEGIN
	DECLARE @MergeDate int, @ReopenDate int= NULL;
	SELECT @MergeDate= ShamsiInt FROM TblDate WHERE MiladiDate =@MergeDateM;
	SELECT @ReopenDate= ShamsiInt FROM TblDate WHERE MiladiDate =@ReopenDateM;

	BEGIN TRY
		INSERT INTO BranchMergeInfo( BranchCodeFrom, BranchCodeTo, MergeDateM, MergeDateS, ReopenDateM, ReopenDateS, MergeDate, ReopenDate ) 
		VALUES( @BranchCodeFrom, @BranchCodeTo, @MergeDateM, dbo.FN_MiladiToShamsi( @MergeDateM ) , @ReopenDateM, dbo.FN_MiladiToShamsi(@ReopenDateM),@MergeDate, @ReopenDate  );
		RETURN SCOPE_IDENTITY();
	END TRY
	BEGIN CATCH 
		RETURN -1;
	END CATCH
	
END;



GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_Commands]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_Insert_Commands](@CommandCode int, @CommandTitle  nvarchar(4000), @Status Int) AS
BEGIN
	INSERT INTO Commands( CommandCode, CommandTitle, Status) 
	VALUES(@CommandCode, @CommandTitle, @Status)
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_ExcelImportLOG]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_ExcelImportLOG]( @RunDate as datetime, @FileNamePath as nvarchar(1000), @SheetName as nvarchar(100), @FetchDateS as char(10), @LedgerId as nvarchar(20), @Status as int) AS
BEGIN

	INSERT INTO Log_ExcelImport(RunDate, FileNamePath, SheetName, FetchDateS , LedgerId, Status )
	VALUES(@RunDate , @FileNamePath , @SheetName , @FetchDateS , @LedgerId , @Status)
	RETURN SCOPE_IDENTITY()

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_Formulas]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_Formulas](
					@SeqNO int, @FormulaType tinyint, @Name nvarchar(200), @Code nvarchar(100), 
					@MethodDesc nvarchar(max), @Context nvarchar(4000), @GeneralId int, 
					@FromLedger nvarchar(4000),@ToLedger nvarchar(4000),@IncludeLedgers nvarchar(4000),					
					@CreateDateM date, @Comment nvarchar(4000), @BranchesIdList nvarchar(max),
					@BranchesNameList nvarchar(max), @BranchTotalList nvarchar(max),
					@WhereClause nvarchar(max), @UsedLedgers nvarchar(max), @UsedLedgersAbr nvarchar(max),
					@UserId int
								) AS
BEGIN

	DECLARE @Ret int, @FormulaId int;
	DECLARE @ErrTitle nvarchar(4000), @CompressedFormulaValue nvarchar(4000), @MyGUID as uniqueidentifier= NEWID();
	SELECT @Ret= dbo.FN_CheckNewFormula( 0, @SeqNO, @Name, @Code );

	
	IF @Ret <> 0 
		RETURN @Ret;

	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION 
		BEGIN TRY
			INSERT INTO Formulas( 
								SeqNO, 
								FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, Comment,
								CreateDateM, CreateDateS, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, 
								UsedLedgers, UsedLedgersAbr, UserId, OwnGUID
								)
			VALUES( 
								CASE @SeqNO WHEN -1 THEN (SELECT ISNULL(Max(SeqNO),0)+10 FROM Formulas ) ELSE @SeqNO END, 
								@FormulaType, @Name, @Code, @MethodDesc, @Context, @GeneralId, NULLIF(@FromLedger,''), NULLIF(@ToLedger,''), 
								NULLIF(@IncludeLedgers,''), @Comment, @CreateDateM, dbo.FN_MiladiToShamsi(@CreateDateM),@BranchesIdList, @BranchesNameList, @BranchTotalList, 
								@WhereClause, @UsedLedgers, @UsedLedgersAbr,@UserId, @MyGUID
					);

			SET @FormulaId = SCOPE_IDENTITY() 
		END TRY
		BEGIN CATCH
			ROLLBACK TRAN;
			RETURN -1;
		END CATCH	

	EXEC USP_Generate_CompressedFormulaForById @FormulaId,1, @ErrTitle OUT, @CompressedFormulaValue OUT;
	
	UPDATE	Formulas SET CompressedFormula= @CompressedFormulaValue WHERE FormulaId =@FormulaId
		
	IF CHARINDEX('Divide by zero error encountered',@ErrTitle,1)=0
	BEGIN
		IF TRY_CONVERT(Decimal(18,4),@ErrTitle) IS NULL
		BEGIN
			ROLLBACK TRAN;
			RETURN -2;
		END
		ELSE	
			COMMIT TRAN
	END	
	ELSE	
		COMMIT TRAN

	INSERT INTO LOG_Formulas
		( ActionDate,LogId,ActType,FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, OwnGUID)
	SELECT 
			GETDATE(),(NEXT VALUE FOR dbo.LogSequence ),'Inserted',FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, NEWID()
	FROM Formulas
	WHERE FormulaId =@FormulaId

	RETURN @FormulaId;
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_ImportExcelSchema]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_ImportExcelSchema]( @LedgerId int,  @UNC nvarchar(1000),@FolderName nvarchar(1000),@SheetName nvarchar(1000)) AS
BEGIN
	INSERT INTO ImportSchemas(LedgerId, UNC, FolderName, SheetName) 
	VALUES( @LedgerId, @UNC, @FolderName, @SheetName)

	RETURN SCOPE_IDENTITY()
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_ImportSchedule]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_ImportSchedule]( @UserId int, @FormulaId int,  @Title nvarchar(100),@FromDateM Date,@ToDateM Date, @ImportMode tinyint, @ImportStatus tinyint) AS
BEGIN
	DECLARE @Ret int=0;
	SELECT @Ret = dbo.FN_ConfilctScheduleDate(@FromDateM,@ToDateM,@FormulaId);
	IF @RET <> 0 
		RETURN 0;

	BEGIN TRY
		INSERT INTO ImportSchedule(UserId, FormulaId,Title,FromDateM,FromDateS,ToDateM,ToDateS,ImportMode,ImportStatus) 
		VALUES( @UserId, @FormulaId, @Title, @FromDateM,dbo.FN_MiladiToShamsi(@FromDateM) , @ToDateM, dbo.FN_MiladiToShamsi(@ToDateM), @ImportMode, @ImportStatus);
		RETURN SCOPE_IDENTITY();
	END TRY
	BEGIN CATCH 
		RETURN -1;
	END CATCH
	
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_UserAccessList]    Script Date: 7/26/2020 1:33:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_UserAccessList]( @UserId int, @AccessIdList nvarchar(1000)) AS
BEGIN
	DELETE UserAccesses Where UserId = @UserId;
	Insert Into UserAccesses(UserId, AccessId )
	Select @UserId, Item from dbo.FN_StringToTable_Not_NULL(@AccessIdList,',')
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_UserGroups]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_UserGroups]( @GroupName as nvarchar(50)) AS
BEGIN
	INSERT INTO UserGroups(GroupName) VALUES(@GroupName)
	RETURN SCOPE_IDENTITY()
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Insert_Users]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Insert_Users]( @UserName nvarchar(50), @UserPass nvarchar(50), @Title nvarchar(100), @AccessMode int, @UserGroupId int ) AS
BEGIN
	INSERT INTO Users( UserName, UserPass, Title, AccessMode, UserGroupId) VALUES( @UserName, NULLIF(@UserPass,''), @Title, @AccessMode, @UserGroupId )
	RETURN SCOPE_IDENTITY() 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_ModifyCodeInDriverFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ModifyCodeInDriverFormula]( @Code NVARCHAR(100) , @NewCode NVARCHAR(100)) AS
BEGIN
	UPDATE Formulas 
	SET 
		MethodDesc = REPLACE(MethodDesc,'['+@Code+']','['+@NewCode+']'  ),
		Context = REPLACE(Context,'['+@Code+']','['+@NewCode+']'  ),
		IncludeLedgers = REPLACE(REPLACE( REPLACE( ';,'+IncludeLedgers+',;' , ','+@Code+',', ','+@NewCode+',' ) , ';,','') ,',;','') , 
		UsedLedgersAbr = REPLACE(REPLACE( REPLACE( ';,'+UsedLedgersAbr+',;' , ','+@Code+',', ','+@NewCode+',' ) , ';,','') ,',;','')  
	FROM Formulas
	WHERE MethodDesc like '%;['+@Code+';]%' ESCAPE ';' 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_MoveSeqForward]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_MoveSeqForward]( @SeqNO int, @NO int= 1 ) AS
BEGIN
	--DECLARE	@SeqNO int, @NO int= 1;
	
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION 
	
	IF dbo.FN_IsTableLockedSerializable( 'Formulas' )= 1 
	BEGIN
		ROLLBACK TRAN
		RETURN -1
		PRINT -1
	END
	ELSE
	BEGIN
		BEGIN TRY
			UPDATE Formulas 
				SET SeqNO =  SeqNO + @NO
			WHERE SeqNO>=@SeqNO;

			COMMIT TRAN
			RETURN 0
			PRINT 0
		END TRY
		BEGIN CATCH
			ROLLBACK TRAN
			RETURN -1
			PRINT -1
		END CATCH
	END;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_ReCalculate_Formula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ReCalculate_Formula]( @FormulaId int, @RunFormula tinyint ) AS
BEGIN
	--DECLARE @FormulaId int;
	DECLARE @FormulaType tinyint, @GeneralId int, @FromLedgers nvarchar(max), @ToLedgers nvarchar(max);
	DECLARE @ErrTitle nvarchar(4000), @CompressedFormulaValue nvarchar(Max);

	DECLARE 
		@LedgerId      nvarchar(Max)='',
		@LedgerAbr     nvarchar(Max)='',
		@LedgerAbrPlus nvarchar(Max)='',
		@LedgerTotal   nvarchar(Max)='';

	DECLARE @RemoveComments TinyInt=0;

	SELECT TOP 1  @RemoveComments=RemoveComments FROM Configs;

	SELECT @FormulaType= FormulaType, @GeneralId = GeneralId, @FromLedgers=FromLedger, @ToLedgers=ToLedger FROM Formulas WHERE FormulaId = @FormulaId;
	
	IF @FormulaType =0 RETURN;

	IF @FormulaType IN(1,2) 
	BEGIN

		SELECT 
			@LedgerTotal = Concat( @LedgerTotal,'[',LedgerTotal,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerId = Concat(@LedgerId, Case @LedgerId when '' then '' else ',' end ,LedgerId)
			,
			@LedgerAbr = Concat(@LedgerAbr, Case @LedgerAbr when '' then '' else ',' end ,LedgerTotal)
			,
			@LedgerAbrPlus = Concat(@LedgerAbrPlus, Case @LedgerAbrPlus when '' then '[' else '+[' end ,LedgerTotal,']')
		FROM VW_GeneralActiveLedgers
		WHERE GeneralId = @GeneralId AND ( @FormulaType =2 OR  LedgerCode BETWEEN  @FromLedgers AND @ToLedgers)

		UPDATE	Formulas
			SET
				MethodDesc		=	LTRIM(RTRIM(@LedgerTotal))	, 
				Context			=	LTRIM(RTRIM(@LedgerAbrPlus)), 
				UsedLedgers		=	LTRIM(RTRIM(@LedgerId))		, 
				UsedLedgersAbr	=	LTRIM(RTRIM(@LedgerAbr))	
		WHERE 	FormulaId		=@FormulaId
	END

	SET NOCOUNT ON;
	EXEC USP_Generate_CompressedFormulaForById @FormulaId, @RunFormula, @ErrTitle OUT, @CompressedFormulaValue OUT;
	
	UPDATE	Formulas SET CompressedFormula= @CompressedFormulaValue WHERE FormulaId =@FormulaId;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_ReCalculate_Formula_ForChangingActiveLedgers]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ReCalculate_Formula_ForChangingActiveLedgers] AS
BEGIN
	BEGIN TRANSACTION	
		DECLARE @FormulaId int;
				
		DECLARE Cur_Formula CURSOR	FOR  
			SELECT FormulaId FROM Formulas WHERE FormulaType in (1,2,3);
				
		OPEN Cur_Formula  
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
		SET NOCOUNT ON;
		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			EXEC USP_ReCalculate_Formula_NoCompressed @FormulaId;
			FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
		END  

		CLOSE Cur_Formula  
		DEALLOCATE Cur_Formula
	COMMIT	

END;



GO

/****** Object:  StoredProcedure [dbo].[USP_ReCalculate_Formula_ForDrived]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_ReCalculate_Formula_ForDrived]( @Code nvarchar(100)) AS
BEGIN
	SET NOCOUNT ON;	
	BEGIN TRANSACTION	

		DECLARE @FormulaId int;
			
		DECLARE Cur_Formula CURSOR	FOR  
			SELECT FormulaId FROM Formulas 
			WHERE FormulaType =3 and FormulaId in (Select FormulaId from FN_Select_Drived_Formula() WHERE Lst ='['+@Code+']');
				
		OPEN Cur_Formula  
		FETCH NEXT FROM Cur_Formula INTO @FormulaId; 

		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			EXEC USP_ReCalculate_Formula @FormulaId,0;
			FETCH NEXT FROM Cur_Formula INTO @FormulaId; 
		END  

		CLOSE Cur_Formula  
		DEALLOCATE Cur_Formula
	COMMIT	

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_ReCalculate_Formula_NoCompressed]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ReCalculate_Formula_NoCompressed]( @FormulaId int ) AS
BEGIN
	--DECLARE @FormulaId int;
	DECLARE @FormulaType tinyint, @GeneralId int, @FromLedgers nvarchar(max), @ToLedgers nvarchar(max);
	DECLARE @ErrTitle nvarchar(4000), @CompressedFormulaValue nvarchar(Max);

	DECLARE 
		@LedgerId      nvarchar(Max)='',
		@LedgerAbr     nvarchar(Max)='',
		@LedgerAbrPlus nvarchar(Max)='',
		@LedgerTotal   nvarchar(Max)='';

	DECLARE @RemoveComments TinyInt=0;

	SELECT TOP 1  @RemoveComments=RemoveComments FROM Configs;

	SELECT @FormulaType= FormulaType, @GeneralId = GeneralId, @FromLedgers=FromLedger, @ToLedgers=ToLedger FROM Formulas WHERE FormulaId = @FormulaId;
	
	IF @FormulaType =0 RETURN;

	IF @FormulaType IN(1,2) 
	BEGIN

		SELECT 
			@LedgerTotal = Concat( @LedgerTotal,'[',LedgerTotal,'] ',CASE @RemoveComments WHEN 2 THEN '' ELSE '/*'+LedgerTitle+'*/' END,Char(13))
			,
			@LedgerId = Concat(@LedgerId, Case @LedgerId when '' then '' else ',' end ,LedgerId)
			,
			@LedgerAbr = Concat(@LedgerAbr, Case @LedgerAbr when '' then '' else ',' end ,LedgerTotal)
			,
			@LedgerAbrPlus = Concat(@LedgerAbrPlus, Case @LedgerAbrPlus when '' then '[' else '+[' end ,LedgerTotal,']')
		FROM VW_GeneralActiveLedgers
		WHERE GeneralId = @GeneralId AND ( @FormulaType =2 OR  LedgerCode BETWEEN  @FromLedgers AND @ToLedgers)

		UPDATE	Formulas
			SET
				MethodDesc		=	LTRIM(RTRIM(@LedgerTotal))	, 
				Context			=	LTRIM(RTRIM(@LedgerAbrPlus)), 
				UsedLedgers		=	LTRIM(RTRIM(@LedgerId))		, 
				UsedLedgersAbr	=	LTRIM(RTRIM(@LedgerAbr))	
		WHERE 	FormulaId		=@FormulaId
	END

	SET NOCOUNT ON;
	--EXEC USP_Generate_CompressedFormulaForById @FormulaId, @RunFormula, @ErrTitle OUT, @CompressedFormulaValue OUT;
	--UPDATE	Formulas SET CompressedFormula= @CompressedFormulaValue WHERE FormulaId =@FormulaId;
END;


GO

/****** Object:  StoredProcedure [dbo].[USP_ReconfigReportsForFetch]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ReconfigReportsForFetch] AS
BEGIN
		EXEC USP_SYS_Modify_LogFormula;
		;WITH CTE AS
		(
			SELECT
				DT.MiladiDate, DT.Shamsi
			FROM ImportSchedule SC
			INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0 
			LEFT  JOIN LOG_Formulas LF ON LF.FormulaId = SC.FormulaId  AND ActType in('Inserted','UInserted')
			WHERE ActionDate = ( SELECT Max(ActionDate)  From LOG_Formulas Where LF.FormulaId = FormulaId and CAST(ActionDate as DATE) <= MiladiDate )
		)
		DELETE R FROM Reports R INNER JOIN CTE C ON R.FetchDateM = C.MiladiDate;

		;WITH CTE AS
		(
			SELECT
				DT.MiladiDate, DT.Shamsi
			FROM ImportSchedule SC
			INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0 
			LEFT  JOIN LOG_Formulas LF ON LF.FormulaId = SC.FormulaId  AND ActType in('Inserted','UInserted')
			WHERE ActionDate = ( SELECT Max(ActionDate)  From LOG_Formulas Where LF.FormulaId = FormulaId and CAST(ActionDate as DATE) <= MiladiDate )
		)
		
		INSERT INTO Reports( ActionDateM,ActionDateS,FetchDateM,FetchDateS,Status,MsgError,ReTry, Backward )
		SELECT DISTINCT
			GETDATE(), dbo.FN_MiladiToShamsi(GETDATE()), MiladiDate, Shamsi, 0 ,NULL, 0, 1  
		FROM CTE 
		WHERE MiladiDate not in (SELECT FetchDateM FROM Reports )
		-- محاسبه گردد Backward=1  نوع محاسبه درخواستي از هر نوعي باشد آنروز بايد با
		-- شود Reports.Backward =1 وجود دارد بايد  ImportSchedule به همين دليل به ازاي هر رکوردي که در 
		-- مهم نيست ImportMode پس مقدار 
		
		TRUNCATE TABLE CalculationLogs;
		INSERT INTO CalculationLogs
		SELECT	FetchDateM,FetchDateS,Status,Backward FROM Reports WHERE Status=0 OR Retry = 1
END;




GO

/****** Object:  StoredProcedure [dbo].[USP_Remove_ActiceLedgers_ByList]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Remove_ActiceLedgers_ByList]( @LedgerList nvarchar(max)) AS
BEGIN

	SELECT 
	  Item as LedgerId
	INTO #TempLedgers
	FROM dbo.FN_StringToTable_Not_NULL( @LedgerList, ',' )
	INTERSECT 
	SELECT 
	  Item
	FROM Formulas
	CROSS APPLY  dbo.FN_StringToTable_Not_NULL(UsedLedgers,',') ;


	DELETE ActiveLedgers
	WHERE LedgerId in(
						SELECT 
						  Item as LedgerId
						FROM dbo.FN_StringToTable_Not_NULL(@LedgerList,',')
						INTERSECT 
						SELECT 
						  Item
						FROM Formulas
						CROSS APPLY  dbo.FN_StringToTable_Not_NULL(UsedLedgers,',')
					);

	SELECT TotalTitle FROM #TempLedgers T
	INNER JOIN VW_GeneralLedgerCoding L ON L.LedgerId = T.LedgerId

	DROP TABLE #TempLedgers

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_RemoveCommentsInFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Batch submitted through debugger: SQLQuery10.sql|0|0|C:\Users\naser\AppData\Local\Temp\~vsCA6C.sql
CREATE PROC [dbo].[USP_RemoveCommentsInFormula]( @Formula as nvarchar(max), @FormulaId as int ) AS
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
	SELECT NULLIF(@FormulaValue,'') Formula , @Idx1+@Idx2+@Idx3 ErrorCode
END









GO

/****** Object:  StoredProcedure [dbo].[USP_RenameCodeInLog]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RenameCodeInLog] (@FromCode nvarchar(100), @ToCode nvarchar(100)) AS
BEGIN
	--DECLARE @FromCode nvarchar(100)='Deb1', @ToCode nvarchar(100)='Deb11' ;
	BEGIN TRAN
		BEGIN TRY
			UPDATE FormulaRemain	SET Code = @ToCode	WHERE Code = @FromCode;
			UPDATE ResultFormulas	SET Code = @ToCode	WHERE Code = @FromCode;

			UPDATE Log_Formulas
			SET UsedLedgersAbr = REPLACE(','+REPLACE( ','+UsedLedgersAbr+',' ,','+@FromCode+',',','+@ToCode+',')+',',',,','')
			WHERE ','+UsedLedgersAbr+','  like '%,'+@FromCode+',%'

			UPDATE Log_Formulas 	SET  MethodDesc			= REPLACE(MethodDesc,'['+@FromCode+']','['+@ToCode+']')
			UPDATE Log_Formulas 	SET  Context			= REPLACE(Context,'['+@FromCode+']','['+@ToCode+']')
			UPDATE Log_Formulas 	SET  CompressedFormula	= REPLACE(CompressedFormula,'['+@FromCode+']','['+@ToCode+']')
		END TRY
		BEGIN CATCH
			ROLLBACK TRAN;
			RETURN -1
		END CATCH	

	COMMIT TRAN
	RETURN 1

END


GO

/****** Object:  StoredProcedure [dbo].[USP_ResultCalculationByFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_ResultCalculationByFormula]
		( @FormulaId int,  @ShamsiDateList	nvarchar(Max), @SQLCodePivotView nvarchar(max) OUT) AS
BEGIN
--DECLARE @FormulaId Int=102016;
DECLARE
			@LedgerAbrList			nvarchar(max) ,
			@LedgerCodeList			nvarchar(max) ,
			@LedgerIdList			nvarchar(max) , 
			@Formula				nvarchar(max) , 
			@LedgerAbrListISNULL	nvarchar(max),
			@BranchesIdList			nvarchar(max),
			@WhereClause			nvarchar(max),
			@FormulaCode			nvarchar(100);
	
	SELECT	@SQLCodePivotView ='';
	SELECT	@FormulaCode = Code FROM Formulas WHERE FormulaId= @FormulaId;

	EXEC USP_Seperate_CompressedFormulaById	
			@FormulaId, 
			@LedgerAbrList OUT, 
			@LedgerCodeList OUT, 
			@LedgerIdList OUT, 
			@Formula OUT, 
			@LedgerAbrListISNULL OUT,
			@BranchesIdList OUT


-----------------------------------------------------------------------------
	SET @WhereClause='				WHERE (1=1) ';

	If ISNULL(@ShamsiDateList,'') <> '' 
		SET @WhereClause= @WhereClause + ' AND T.FetchDateS in ('+''+@ShamsiDateList+')' ;

	IF ISNULL(@BranchesIdList,'') <> '' 
		SET @WhereClause= @WhereClause + ' AND T.BranchId in('+@BranchesIdList+') ';
-----------------------------------------------------------------------------

	SET @SQLCodePivotView =

		'SELECT '''+@FormulaCode+''' as Code,PVT.BranchId, BR.BranchName, PVT.FetchDateM, DT.Shamsi, Remain, FirstRemain, LastRemain, CreditValue, DebitValue '+CHAR(13)+
		'FROM ( '+CHAR(13)+
		'	SELECT RemainType, BranchId, FetchDateM,SUM('+@Formula+') as Amount '+CHAR(13)+
		'	FROM'+CHAR(13)+
		'	('+CHAR(13)+
		'		SELECT ''Remain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'				SELECT '+CHAR(13)+
		'					LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'				FROM ResultTrans T '+CHAR(13)+
		'				INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(Remain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT1'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''FirstRemain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'				SELECT '+CHAR(13)+
		'					LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'				FROM ResultTrans T '+CHAR(13)+
		'				INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(FirstRemain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT2'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''LastRemain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'				SELECT '+CHAR(13)+
		'					LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'				FROM ResultTrans T '+CHAR(13)+
		'				INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(LastRemain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT3'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''CreditValue'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'				SELECT '+CHAR(13)+
		'					LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'				FROM ResultTrans T '+CHAR(13)+
		'				INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(CreditValue) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT4'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''DebitValue'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'				SELECT '+CHAR(13)+
		'					LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'				FROM ResultTrans T '+CHAR(13)+
		'				INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(DebitValue) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT5'+CHAR(13)+
		'	)'+CHAR(13)+
		'	K(	 RemainType, BranchId, FetchDateM,'+@LedgerAbrList+') '+CHAR(13)+
		'	GROUP BY RemainType, BranchId, FetchDateM '+CHAR(13)+
		'	) as SRC  PIVOT(  '+CHAR(13)+
		'	SUM(Amount) FOR RemainType IN(Remain,FirstRemain,LastRemain,CreditValue,DebitValue)  '+CHAR(13)+
		') PVT '+CHAR(13)+
		' INNER JOIN Branches BR ON PVT.BranchId = BR.BranchId '+CHAR(13)+
		' LEFT JOIN TblDate DT ON DT.MiladiDate = PVT.FetchDateM '+CHAR(13)
	
	--+' ORDER BY DT.Shamsi DESC '
	--PRINT	@SQLCodePivotView 
	--EXEC( @SQLCodePivotView  )

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTree]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTree]( @FormulaCode nvarchar(100) , @FetchDateM date, @UseBranch int =0 ) AS
BEGIN

	;WITH CTE_REC AS
	(
		SELECT 0 as Level ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE FormulaType = 3  AND ( @FormulaCode ='' OR Code =@FormulaCode  )
		UNION ALL
		SELECT B.Level+1,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_CALC AS
	(
		SELECT 
			DISTINCT
			Level,A.FormulaType,Tree, A.Code, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			, CASE @UseBranch WHEN 0 THEN NULL ELSE BranchId END BranchId ,RF.Remain, RF.CreditValue, RF.DebitValue
		from CTE_REC A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF ON A.Code = RF.Code and RF.FetchDateM = @FetchDateM
	)
	SELECT 
		Code, Tree,Level,SeqNO,FormulaType,Context,BranchId, 
		SUM(Remain) Remain,SUM(CreditValue) CreditValue,SUM(DebitValue) DebitValue
	FROM CTE_CALC
	GROUP BY Code, Tree,Level,SeqNO,FormulaType,Context,BranchId 
	ORDER BY Tree, BranchId 

END









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeBranchDrived]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeBranchDrived]( @FormulaCode nvarchar(100) , @FetchDateM date  ) AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb17]' , @FetchDateM	DATE = '2015-10-20';
	;WITH CTE_BranchesResult AS
	(
		SELECT 
			BR.BranchId, BR.Parent, BR.BranchName, RF.FetchDateM, RF.Code, RF.Remain, RF.CreditValue, RF.DebitValue 
		FROM Branches BR
		LEFT JOIN ResultFormulas RF ON BR.BranchId = RF.BranchId AND RF.FetchDateM = @FetchDateM and  '['+RF.Code+']'= @FormulaCode
	)
	,CTE_REC AS
	(
		SELECT BranchId, BranchName, Remain, CreditValue, DebitValue, BranchId as RootID FROM CTE_BranchesResult A
		UNION ALL
		SELECT A.BranchId, A.BranchName, A.Remain, A.CreditValue, A.DebitValue, B.RootID FROM CTE_BranchesResult A
		INNER JOIN CTE_REC B ON A.Parent = B.BranchId
	)
	,
	CTE_Total AS
	(
		SELECT	
				BR.BranchId, BR.Parent, BR.BranchName, BR.Remain, 
				SUM(T.Remain) as TotalRemain,
				SUM(T.CreditValue) as TotalCreditValue,
				SUM(T.DebitValue) as TotalDebitValue			
		FROM CTE_BranchesResult BR
		LEFT JOIN CTE_REC T ON BR.BranchId = T.RootID
		GROUP BY BR.BranchId, BR.Parent, BR.BranchName, BR.Remain
	)

	,CTE_Final AS
	(
		SELECT BranchId, Parent, 0 Level, BranchName, CAST(BranchName as nvarchar(100)) as Tree, TotalRemain, TotalCreditValue, TotalDebitValue 
		FROM CTE_Total
		WHERE Parent IS NULL
		UNION ALL
		SELECT A.BranchId, A.Parent, Level+1, A.BranchName,CAST(B.Tree+'\'+A.BranchName as nvarchar(100)), A.TotalRemain, A.TotalCreditValue, A.TotalDebitValue 
		FROM CTE_Total A
		INNER JOIN CTE_Final B ON B.BranchId = A.Parent
	)
	SELECT 
		BranchId,Parent,Level,BranchName,Tree,TotalRemain as Remain,TotalCreditValue as CreditValue,TotalDebitValue as DebitValue
	FROM CTE_Final
	ORDER BY Tree, BranchId 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeBranchDrivedRange]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeBranchDrivedRange]( @FormulaCode nvarchar(100) , @FetchDateMFrom date, @FetchDateMTo DATE ) AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb1]' , @FetchDateMFrom	DATE = '2015-10-21', @FetchDateMTo DATE = '2015-10-22';
	BEGIN TRY DROP TABLE #CTE_BranchesResult	END TRY BEGIN CATCH END CATCH;
	;WITH CTE_BranchesResultPre AS
	(
		SELECT 
			BR.BranchId, BR.Parent, BR.BranchName, RF.FetchDateM, RF.FirstRemain, RF.LastRemain, RF.CreditValue, RF.DebitValue 
		FROM Branches BR
		LEFT JOIN ResultFormulas RF ON BR.BranchId = RF.BranchId AND RF.FetchDateM BETWEEN @FetchDateMFrom and @FetchDateMTo and  '['+RF.Code+']'= @FormulaCode
	) 
	,CTE_BranchesResult AS
	(
		SELECT DISTINCT
			BranchId, Parent, BranchName,
			FIRST_VALUE(FirstRemain) OVER (PARTITION BY BranchId ORDER BY FetchDateM) FirstRemain,
			SUM(CreditValue) OVER (PARTITION BY BranchId) CreditValue,
			SUM(DebitValue) OVER (PARTITION BY BranchId) DebitValue,
			LAST_VALUE(LastRemain) OVER (PARTITION BY BranchId ORDER BY FetchDateM  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LastRemain
		FROM CTE_BranchesResultPre
	)
	SELECT 
		BranchId, Parent, BranchName, FirstRemain, LastRemain, CreditValue, DebitValue
	into #CTE_BranchesResult from CTE_BranchesResult;

	;With CTE_REC AS
	(
		SELECT BranchId, BranchName, FirstRemain, LastRemain, CreditValue, DebitValue, BranchId as RootID FROM #CTE_BranchesResult A
		UNION ALL
		SELECT A.BranchId, A.BranchName, A.FirstRemain, A.LastRemain, A.CreditValue, A.DebitValue, B.RootID FROM #CTE_BranchesResult A
		INNER JOIN CTE_REC B ON A.Parent = B.BranchId
	) 
	,CTE_Total AS
	(
		SELECT	
				BR.BranchId, BR.Parent, BR.BranchName,
				SUM(T.FirstRemain) as TotalFirstRemain,
				SUM(T.LastRemain) as TotalLastRemain,
				SUM(T.CreditValue) as TotalCreditValue,
				SUM(T.DebitValue) as TotalDebitValue			
		FROM #CTE_BranchesResult BR
		LEFT JOIN CTE_REC T ON BR.BranchId = T.RootID
		GROUP BY BR.BranchId, BR.Parent, BR.BranchName, BR.FirstRemain, BR.LastRemain
	) 		
	, CTE_Final AS
	(
		SELECT BranchId, Parent, 0 Level, BranchName, CAST(BranchName as nvarchar(100)) as Tree, TotalFirstRemain, TotalLastRemain, TotalCreditValue, TotalDebitValue 
		FROM CTE_Total
		WHERE Parent IS NULL
		UNION ALL
		SELECT A.BranchId, A.Parent, Level+1, A.BranchName,CAST(B.Tree+'\'+A.BranchName as nvarchar(100)), A.TotalFirstRemain, A.TotalLastRemain, A.TotalCreditValue, A.TotalDebitValue 
		FROM CTE_Total A
		INNER JOIN CTE_Final B ON B.BranchId = A.Parent
	)
	SELECT Top 100000000
		BranchId,Parent,Level,BranchName,Tree,
		TotalFirstRemain as FirstRemain,
		TotalLastRemain as LastRemain,
		TotalCreditValue as CreditValue,
		TotalDebitValue as DebitValue
	FROM CTE_Final
	ORDER BY Tree, BranchId;
	BEGIN TRY DROP TABLE #CTE_BranchesResult	END TRY BEGIN CATCH END CATCH;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeFormulaDrived]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeFormulaDrived]( @FormulaCode nvarchar(100) , @FetchDateM date) AS
BEGIN
	;WITH CTE_REC AS
	(
		SELECT 0 as Level ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE  ( @FormulaCode ='' OR Code =@FormulaCode  ) --AND FormulaType = 3
		UNION ALL
		SELECT B.Level+1,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_CALC AS
	(
		SELECT 
			DISTINCT
			Level,A.FormulaType,Tree, A.Code, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			,RF.FirstRemain, RF.CreditValue, RF.DebitValue,RF.LastRemain
		from CTE_REC A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF ON A.Code = RF.Code and RF.FetchDateM = @FetchDateM
	)
	SELECT 
		Code, Tree,Level,SeqNO,FormulaType,Context, 
		SUM(LastRemain) Remain,SUM(FirstRemain) FirstRemain,SUM(CreditValue) CreditValue,SUM(DebitValue) DebitValue,SUM(LastRemain) LastRemain
	FROM CTE_CALC
	GROUP BY Code, Tree,Level,SeqNO,FormulaType,Context 
	ORDER BY Tree 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeFormulaDrivedRange]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeFormulaDrivedRange]( @FormulaCode nvarchar(100) , @FetchDateMFrom date , @FetchDateMTo date) WITH RECOMPILE  AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb70]' , @FetchDateMFrom	DATE = '2014-04-21', @FetchDateMTo DATE = '2014-04-29';
	;WITH CTE_REC AS
	(
		SELECT 0 as Level ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE  ( @FormulaCode ='' OR Code =@FormulaCode  ) 
		UNION ALL
		SELECT B.Level+1,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_FormulaTree AS
	(
		SELECT DISTINCT Level ,Tree,FormulaType,Code,SeqNO FROM CTE_REC  
	),
	CTE_CALC AS
	(
		SELECT 
			FetchDateM,Level,A.FormulaType,Tree, A.Code, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			,RF.FirstRemain, RF.CreditValue, RF.DebitValue,RF.LastRemain
		FROM CTE_FormulaTree A
		INNER JOIN Formulas F (nolock) ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF (nolock) ON A.Code = RF.Code and RF.FetchDateM Between @FetchDateMFrom and @FetchDateMTo
	),
	CTE_NEW AS
	(
	SELECT TOP 100000000
		FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context, 
		SUM(FirstRemain) FirstRemain,
		SUM(CreditValue) CreditValue,
		SUM(DebitValue) DebitValue,
		SUM(LastRemain) LastRemain
	FROM CTE_CALC
	GROUP BY FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context
	ORDER BY Tree 
	)

	SELECT DISTINCT
		CASE WHEN B.Code IS NULL THEN 0 ELSE 1 END as LogStatus,
		F.FormulaId, F.Name,
		A.Code, Tree,Level, A.SeqNO, A.FormulaType, A.Context,
		FIRST_VALUE(FirstRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM) FirstRemain,
		SUM(CreditValue) OVER (PARTITION BY A.Code) CreditValue,
		SUM(DebitValue) OVER (PARTITION BY A.Code) DebitValue,
		LAST_VALUE(LastRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LastRemain
	FROM CTE_NEW A
	LEFT JOIN VW_FormulasIncludeLOG B ON A.Code = B.Code 
	LEFT JOIN Formulas F (nolock) ON A.Code = F.Code 
	ORDER BY Tree 
END


GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeFormulaDrivedRange_OLD]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeFormulaDrivedRange_OLD]( @FormulaCode nvarchar(100) , @FetchDateMFrom date , @FetchDateMTo date) WITH RECOMPILE  AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb1]' , @FetchDateMFrom	DATE = '2015-10-21', @FetchDateMTo DATE = '2015-10-22';
	;WITH CTE_REC AS
	(
		SELECT 0 as Level ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE  ( @FormulaCode ='' OR Code =@FormulaCode  ) 
		UNION ALL
		SELECT B.Level+1,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_CALC AS
	(
		SELECT 
			DISTINCT
			FetchDateM,Level,A.FormulaType,Tree, A.Code, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			,RF.FirstRemain, RF.CreditValue, RF.DebitValue,RF.LastRemain
		from CTE_REC A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF ON A.Code = RF.Code and RF.FetchDateM Between @FetchDateMFrom and @FetchDateMTo
	),
	CTE_NEW AS
	(
	SELECT TOP 100000000
		FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context, 
		SUM(FirstRemain) FirstRemain,
		SUM(CreditValue) CreditValue,
		SUM(DebitValue) DebitValue,
		SUM(LastRemain) LastRemain
	FROM CTE_CALC
	GROUP BY FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context
	ORDER BY Tree 
	)

	SELECT DISTINCT
		CASE WHEN B.Code IS NULL THEN 0 ELSE 1 END as LogStatus,
		F.FormulaId,
		A.Code, Tree,Level, A.SeqNO, A.FormulaType, A.Context,
		FIRST_VALUE(FirstRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM) FirstRemain,
		SUM(CreditValue) OVER (PARTITION BY A.Code) CreditValue,
		SUM(DebitValue) OVER (PARTITION BY A.Code) DebitValue,
		LAST_VALUE(LastRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LastRemain
	FROM CTE_NEW A
	LEFT JOIN VW_FormulasIncludeLOG B ON A.Code = B.Code 
	LEFT JOIN Formulas F ON A.Code = F.Code 
	ORDER BY Tree 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeFormulaDrivedRange2]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeFormulaDrivedRange2]( @FormulaCode nvarchar(100) , @FetchDateMFrom date , @FetchDateMTo date) WITH RECOMPILE  AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb1]' , @FetchDateMFrom	DATE = '2015-10-21', @FetchDateMTo DATE = '2015-10-22';
	;WITH CTE_REC AS
	(
		SELECT 0 as Level ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE  ( @FormulaCode ='' OR Code =@FormulaCode  ) 
		UNION ALL
		SELECT B.Level+1,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_CALC AS
	(
		SELECT 
			DISTINCT
			FetchDateM,Level,A.FormulaType,Tree, A.Code, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			,RF.FirstRemain, RF.CreditValue, RF.DebitValue,RF.LastRemain
		from CTE_REC A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF ON A.Code = RF.Code and RF.FetchDateM Between @FetchDateMFrom and @FetchDateMTo
	),
	CTE_NEW AS
	(
	SELECT TOP 100000000
		FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context, 
		SUM(FirstRemain) FirstRemain,
		SUM(CreditValue) CreditValue,
		SUM(DebitValue) DebitValue,
		SUM(LastRemain) LastRemain
	FROM CTE_CALC
	GROUP BY FetchDateM, Code, Tree,Level,SeqNO,FormulaType,Context
	ORDER BY Tree 
	)

	SELECT DISTINCT
		CASE WHEN B.Code IS NULL THEN 0 ELSE 1 END as LogStatus,
		F.FormulaId,
		A.Code, Tree,Level, A.SeqNO, A.FormulaType, A.Context,
		FIRST_VALUE(FirstRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM) FirstRemain,
		SUM(CreditValue) OVER (PARTITION BY A.Code) CreditValue,
		SUM(DebitValue) OVER (PARTITION BY A.Code) DebitValue,
		LAST_VALUE(LastRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LastRemain
	FROM CTE_NEW A
	LEFT JOIN VW_FormulasIncludeLOG B ON A.Code = B.Code 
	LEFT JOIN Formulas F ON A.Code = F.Code 
	ORDER BY Tree 
END


GO

/****** Object:  StoredProcedure [dbo].[USP_RPT_ResultTreeFormulaDrivedRangeTest]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RPT_ResultTreeFormulaDrivedRangeTest]( @FormulaCode nvarchar(100) , @FetchDateMFrom date , @FetchDateMTo date) WITH RECOMPILE  AS
BEGIN
--DECLARE @FormulaCode	NVARCHAR(100) = '[Deb1]' , @FetchDateMFrom	DATE = '2014-04-21', @FetchDateMTo DATE = '2014-04-21';
	;WITH CTE_REC AS
	(
		SELECT 0 as Level, CAST(NULL as nvarchar(4000)) as Parent ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas  
		WHERE  ( @FormulaCode ='' OR Code =@FormulaCode  ) 
		UNION ALL
		SELECT B.Level+1, B.Code ,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	),
	CTE_FormulaTree AS
	(
		SELECT DISTINCT Level ,Tree,FormulaType,Code, Parent,SeqNO FROM CTE_REC  
	),
	CTE_CALC AS
	(
		SELECT 
			FetchDateM,Level,A.FormulaType,Tree, A.Code, A.Parent, A.SeqNO , Case A.FormulaType WHEN 3 THEN F.Context ELSE NULL END as Context
			,RF.FirstRemain, RF.CreditValue, RF.DebitValue,RF.LastRemain
		from CTE_FormulaTree A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
		LEFT MERGE JOIN ResultFormulas RF ON A.Code = RF.Code and RF.FetchDateM Between @FetchDateMFrom and @FetchDateMTo
	),
	CTE_NEW AS
	(
	SELECT TOP 100000000
		FetchDateM, Code, Parent, Tree,Level,SeqNO,FormulaType,Context, 
		SUM(FirstRemain) FirstRemain,
		SUM(CreditValue) CreditValue,
		SUM(DebitValue) DebitValue,
		SUM(LastRemain) LastRemain
	FROM CTE_CALC
	GROUP BY FetchDateM, Code,Parent, Tree,Level,SeqNO,FormulaType,Context
	ORDER BY Tree 
	)

	SELECT DISTINCT
		CASE WHEN B.Code IS NULL THEN 0 ELSE 1 END as LogStatus,
		F.FormulaId,
		A.Code, A.Parent, Tree,Level as Lvl, A.SeqNO, A.FormulaType, A.Context,
		FIRST_VALUE(FirstRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM) FirstRemain,
		SUM(CreditValue) OVER (PARTITION BY A.Code) CreditValue,
		SUM(DebitValue) OVER (PARTITION BY A.Code) DebitValue,
		LAST_VALUE(LastRemain) OVER (PARTITION BY A.Code ORDER BY FetchDateM  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LastRemain
	FROM CTE_NEW A
	LEFT JOIN VW_FormulasIncludeLOG B ON A.Code = B.Code 
	LEFT JOIN Formulas F ON A.Code = F.Code 
	ORDER BY Tree 
END


GO

/****** Object:  StoredProcedure [dbo].[USP_RUN_Commands]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_RUN_Commands] AS
BEGIN
	IF EXISTS(SELECT * FROM Commands WHERE CommandCode =1 and Status =0 )
	BEGIN
		EXEC USP_ReCalculate_Formula_ForChangingActiveLedgers;
		UPDATE Commands SET Status =1 WHERE CommandCode =1
	END
END









GO

/****** Object:  StoredProcedure [dbo].[USP_RUN_Formula_Runtime]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RUN_Formula_Runtime]( @FormulaId int ) AS
BEGIN
	DECLARE @A NVARCHAR(MAX)='';
	EXEC USP_Generate_CalcSQL_ByFormula_Runtime @FormulaId, @A OUT ;
	EXEC(@A )
END;
GO

/****** Object:  StoredProcedure [dbo].[USP_RunPivotTest]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_RunPivotTest](@SQL as nvarchar(max), @Res nvarchar(4000) OUTPUT) AS
BEGIN
	CREATE	TABLE #RESTABLE( Title nvarchar(4000) ) ;
	INSERT INTO #RESTABLE	EXEC( @SQL );
	SELECT @Res = Title FROM #RESTABLE;
END;

GO

/****** Object:  StoredProcedure [dbo].[USP_Select_ActiveLedgerCodes]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_ActiveLedgerCodes] ( @GeneralId int ) AS
BEGIN
	SELECT	TOP 10000000
		LedgerId, GeneralId, SystemId, LedgerCode, LedgerTitle, LedgerAbr, LedgerTotal,
		TotalTitle, GeneralCode, GeneralTitle, GeneralAbr, SystemName, LedgerTitleCode
	FROM	VW_GeneralActiveLedgers
	WHERE (@GeneralId =0 OR GeneralId = @GeneralId)
	ORDER BY GeneralId
END;











GO

/****** Object:  StoredProcedure [dbo].[USP_Select_ActiveLedgerCodesAndFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_ActiveLedgerCodesAndFormula] ( @GeneralId int ) AS
BEGIN

	SELECT	TOP 10000000
		LedgerId, GeneralId, SystemId, LedgerTotal,LedgerTitle,	TotalTitle
	FROM VW_ActiveLedgerCodesAndFormula
	WHERE ( GeneralId = -1 OR @GeneralId =0 OR GeneralId = @GeneralId)
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_BranchMergeInfo]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_BranchMergeInfo] AS
BEGIN
	SELECT 
		BranchMergeInfoId,BranchCodeFrom,BranchCodeTo,MergeDateM,MergeDateS,ReopenDateM,ReopenDateS 
	FROM BranchMergeInfo
END











GO

/****** Object:  StoredProcedure [dbo].[USP_Select_CalculatedResult]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_CalculatedResult]( @Code nvarchar(100),  @ShamsiFrom Char(10) ,  @ShamsiTo Char(10) ) AS 
BEGIN
	SELECT  Code,RF.BranchId, BR.BranchName, RF.FetchDateM, DT.Shamsi, Remain, FirstRemain, LastRemain, CreditValue, DebitValue 
	FROM ResultFormulas RF
	INNER JOIN Branches BR ON RF.BranchId = BR.BranchId 
	INNER JOIN  TblDate DT ON DT.MiladiDate = RF.FetchDateM 
	WHERE Code = @Code 
			AND ( ISNULL(@ShamsiFrom,'')='' OR  Dt.Shamsi >= @ShamsiFrom  )
			AND ( ISNULL(@ShamsiTo,'')='' OR  Dt.Shamsi <= @ShamsiTo  )
	ORDER BY DT.Shamsi DESC 
	
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_CalculatedResult_OLD]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[USP_Select_CalculatedResult_OLD]( @Code nvarchar(100),  @ShamsiFrom Char(10) ,  @ShamsiTo Char(10) ) AS
BEGIN
--DECLARE @FormulaId Int=102016;
DECLARE
			@FormulaId				Int ,
			@LedgerAbrList			nvarchar(max) ,
			@LedgerCodeList			nvarchar(max) ,
			@LedgerIdList			nvarchar(max) , 
			@Formula				nvarchar(max) , 
			@LedgerAbrListISNULL	nvarchar(max),
			@BranchesIdList			nvarchar(max),
			@WhereClause			nvarchar(max);
	
	SELECT @FormulaId = FormulaId   FROM Formulas WHERE Code =@Code ;

	EXEC USE_Seperate_CompressedFormulaById	
			@FormulaId, 
			@LedgerAbrList OUT, 
			@LedgerCodeList OUT, 
			@LedgerIdList OUT, 
			@Formula OUT, 
			@LedgerAbrListISNULL OUT,
			@BranchesIdList OUT

-----------------------------------------------------------------------------
	SET @WhereClause='				WHERE T.FetchDateS BETWEEN '''+@ShamsiFrom+''' AND '''+@ShamsiTo+'''';

	IF ISNULL(@BranchesIdList,'') <> '' 
		SET @WhereClause= 
		'	AND T.BranchId in('+@BranchesIdList+') ';

	DECLARE @SQLCodePivotView nvarchar(max)='';

	SET @SQLCodePivotView =

		'SELECT '''+@Code+''' as Code,PVT.BranchId, BR.BranchName, DT.Shamsi, Remain, FirstRemain, LastRemain, CreditValue, DebitValue FROM ( '+CHAR(13)+
		'	SELECT RemainType, BranchId, FetchDateM,SUM('+@Formula+') as Amount '+CHAR(13)+
		'	FROM'+CHAR(13)+
		'	('+CHAR(13)+
		'		SELECT ''Remain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'			SELECT '+CHAR(13)+
		'				LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'			FROM ResultTrans T '+CHAR(13)+
		'			INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+
		@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(Remain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT1'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''FirstRemain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'			SELECT '+CHAR(13)+
		'				LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'			FROM ResultTrans T '+CHAR(13)+
		'			INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+
		@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(FirstRemain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT2'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''LastRemain'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'			SELECT '+CHAR(13)+
		'				LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'			FROM ResultTrans T '+CHAR(13)+
		'			INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+
		@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(LastRemain) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT3'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''CreditValue'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'			SELECT '+CHAR(13)+
		'				LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'			FROM ResultTrans T '+CHAR(13)+
		'			INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+
		@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(CreditValue) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT4'+CHAR(13)+
		'		UNION ALL '+CHAR(13)+
		'		SELECT ''DebitValue'' as RemainType, BranchId, FetchDateM,'+@LedgerAbrListISNULL+CHAR(13)+
		'		FROM ( '+CHAR(13)+
		'			SELECT '+CHAR(13)+
		'				LedgerTotal, T.BranchId, T.FetchDateM, T.Remain, T.FirstRemain, T.LastRemain, T.CreditValue, T.DebitValue '+CHAR(13)+
		'			FROM ResultTrans T '+CHAR(13)+
		'			INNER JOIN VW_GeneralLedgerCoding GLC ON T.LedgerId = GLC.LedgerId '+CHAR(13)+
		@WhereClause+CHAR(13)+
		'			 ) as TBL '+CHAR(13)+
		'		PIVOT('+CHAR(13)+
		'				SUM(DebitValue) FOR LedgerTotal IN'+CHAR(13)+
		'										('+@LedgerAbrList+')'+CHAR(13)+
		'			) PVT5'+CHAR(13)+
		'	)'+CHAR(13)+
		'	K(	 RemainType, BranchId, FetchDateM,'+@LedgerAbrList+') '+CHAR(13)+
		'	GROUP BY RemainType, BranchId, FetchDateM '+CHAR(13)+
		'	) as SRC  PIVOT(  '+CHAR(13)+
		'	SUM(Amount) FOR RemainType IN(Remain,FirstRemain,LastRemain,CreditValue,DebitValue)  '+CHAR(13)+
		') PVT '+CHAR(13)+
		' INNER JOIN Branches BR ON PVT.BranchId = BR.BranchId '+CHAR(13)+
		' LEFT JOIN TblDate DT ON DT.MiladiDate = PVT.FetchDateM '+CHAR(13)+
		' ORDER BY DT.Shamsi DESC '
		

	--PRINT	@SQLCodePivotView 
	EXEC( @SQLCodePivotView  )

END;




GO

/****** Object:  StoredProcedure [dbo].[USP_Select_DrivedFormulaTo]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_DrivedFormulaTo]( @Code NVARCHAR(200) ) AS
BEGIN
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			FormulaId,SeqNo,  '['+Code+']' as Code, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	 (
		SELECT 
			FormulaId,SeqNo,Code,UsedLedgersAbr2, ITEM as Lst 
		FROM CTE_TEMP B
		CROSS APPLY( SELECT Item FROM dbo.FN_StringToTable_Not_NULL( B.UsedLedgersAbr2,',') ) A 
		WHERE Item IN ( SELECT Code FROM CTE_TEMP )
	)
	,
	CTE_REC AS
	(
		SELECT SeqNO,Code, Lst FROM CTE_ALL 
		WHERE Code = @Code
		UNION ALL
		SELECT A.SeqNO,A.Code,A.Lst FROM CTE_ALL A
		INNER JOIN CTE_REC B ON B.Lst = A.Code
	)
	SELECT DISTINCT
		F.SeqNO, A.Lst, ISNULL(F.FormulaType,99) as FormulaType  
	FROM CTE_REC A
	LEFT JOIN Formulas F ON A.Lst = '['+F.Code+']'
	ORDER BY F.SeqNO 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_DrivedFormulaToALL]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_DrivedFormulaToALL]( @Code NVARCHAR(200) ) AS
BEGIN
	WITH CTE_TEMP AS
	(
		SELECT TOP 1000000
			FormulaId,SeqNo,  '['+Code+']' as Code, 
			UsedLedgersAbr ,'['+REPLACE( UsedLedgersAbr,',','],[')+']' as UsedLedgersAbr2
		FROM Formulas
		ORDER BY SeqNO
	) ,
	CTE_ALL AS
	 (
		SELECT 
			FormulaId,SeqNo,Code,UsedLedgersAbr2, ITEM as Lst 
		FROM CTE_TEMP B
		CROSS APPLY( SELECT Item FROM dbo.FN_StringToTable_Not_NULL( B.UsedLedgersAbr2,',') ) A 
	)
	,
	CTE_REC AS
	(
		SELECT SeqNO,Code, Lst FROM CTE_ALL 
		WHERE Code = @Code
		UNION ALL
		SELECT A.SeqNO,A.Code,A.Lst FROM CTE_ALL A
		INNER JOIN CTE_REC B ON B.Lst = A.Code
	)
	SELECT DISTINCT
	A.SeqNO, A.Lst, ISNULL(F.FormulaType,99) as FormulaType  
	FROM CTE_REC A
	LEFT JOIN Formulas F ON A.Lst = '['+F.Code+']'
	ORDER BY A.SeqNO 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Formula_ByCode]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Formula_ByCode]( @Code nvarchar(max) ) AS
BEGIN
	SELECT TOP 10000000 F_L, Id,Code, Name,TotalTitle, CommTotalTitle
	FROM VW_UNION_LedgersAndFormula 
	WHERE Code in (SELECT Item FROM dbo.FN_StringToTable(@Code,',') ) 
END
GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Formula_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Formula_ById]( @FormulaId int ) AS
BEGIN

	SELECT  distinct TOP 1000000000
		FR.FormulaId, FR.UserId,FR.SeqNO, FR.FormulaType, 
		CASE FR.FormulaType 
			WHEN  0 THEN N'شامل معين هاي'
			WHEN  1 THEN N'معين از ... تا'
			WHEN  2 THEN N'معين هاي کل'
			WHEN  3 THEN N'دستي'
		END FormulaTypeDesc,		
		FR.Name, FR.Code, FR.MethodDesc, 
		CAST( Context as NVARCHAR(4000)) as  Context,
		FR.GeneralId, G.GeneralTitle, G.GeneralCode,G.SystemId,   
		FR.FromLedger, 
		LF.LedgerTitle as FromLedgerTitle, FR.ToLedger, LT.LedgerTitle as ToLedgerTitle, 
		CAST( FR.IncludeLedgers as NVARCHAR(4000)) as IncludeLedgers, FR.CreateDateM, FR.CreateDateS, 
		FR.ModifyDateM, FR.ModifyDateS, FR.Comment, FR.BranchesIdList, FR.BranchesNameList, FR.BranchTotalList, 
		FR.WhereClause, FR.UsedLedgers, FR.UsedLedgersAbr, ISNULL(IsPrivate,0) as IsPrivate,
		U.AccessMode, U.Title, U.UserName, UserGroupId, 
		CASE WHEN ImportStatus IS NULL THEN N'' ELSE N'دارد' END ImportStatusTitle
	FROM  Formulas FR 
	LEFT JOIN Generals G ON G.GeneralId= FR.GeneralId 
	LEFT JOIN Ledgers LF ON FR.FromLedger = LF.LedgerCode and LF.GeneralId = G.GeneralId
	LEFT JOIN Ledgers LT ON FR.ToLedger = LT.LedgerCode and LT.GeneralId = G.GeneralId
	LEFT JOIN Users    U ON FR.UserId = U.UserId
	LEFT JOIN ImportSchedule SC ON SC.FormulaId = FR.FormulaId
	WHERE ( @FormulaId =0 OR FR.FormulaId=@FormulaId )
	ORDER BY FR.SeqNO
END;



GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Formula_BySeqNO]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [dbo].[USP_Select_Formula_BySeqNO]( @SeqNO int ) AS
BEGIN
	SELECT  TOP 1000000000
		FR.FormulaId, FR.SeqNO, FR.FormulaType, FR.Name, FR.Code, FR.MethodDesc, FR.Context, FR.GeneralId, 
		FR.FromLedger, FR.ToLedger, FR.IncludeLedgers, FR.CreateDateM, FR.CreateDateS, 
		FR.ModifyDateM, FR.ModifyDateS, FR.Comment, FR.BranchesIdList, FR.BranchesNameList, FR.BranchTotalList, 
		FR.WhereClause, FR.UsedLedgers, FR.UsedLedgersAbr
	FROM  Formulas FR 
	WHERE (@SeqNO =0 OR FR.SeqNO=@SeqNO)
	ORDER BY FR.SeqNO
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Formula_ByUserId]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Formula_ByUserId]( @UserId int ) AS
BEGIN
	SELECT 
		FormulaId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, 
		IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, 
		BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, UserID
	  FROM VW_Formulas

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_FormulaTitle]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_FormulaTitle]( @AbrList nvarchar(max) ) AS
BEGIN
	SELECT Name as Title, Code  as Abr 
	FROM Formulas F
	INNER JOIN dbo.FN_StringToTable_Not_NULL(@AbrList,',') B ON F.Code = B.Item
END
GO

/****** Object:  StoredProcedure [dbo].[USP_Select_GeneralActiveCodes]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_GeneralActiveCodes](@SystemId int, @Activate int) AS
BEGIN
	SELECT
		GC.GeneralId, GC.SystemId, GC.GeneralCode, GC.GeneralTitle, GC.GeneralAbr, GC.SystemName,
		GC.GeneralTitle +'('+ Cast(GC.GeneralCode as nvarchar(50))+')' as GeneralTitleCode
	FROM	VW_GeneralCoding GC
	WHERE (@SystemId =0 OR SystemId = @SystemId) AND EXISTS ( SELECT 1 FROM VW_GeneralActiveLedgers AL WHERE (@Activate=0 OR AL.GeneralId = GC.GeneralId) )
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_GeneralCodes]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_GeneralCodes](@SystemId int) AS
BEGIN
	SELECT
		GeneralId, SystemId, GeneralCode, GeneralTitle, GeneralAbr, SystemName,
		GeneralTitle +'('+ Cast(GeneralCode as nvarchar(50))+')' as GeneralTitleCode
	FROM	VW_GeneralCoding
	WHERE (@SystemId =0 OR SystemId = @SystemId)
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Generals_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Generals_ById]( @GeneralId int ) AS
BEGIN
	SELECT 
		GC.GeneralId, GC.SystemId, GC.GeneralCode, GC.GeneralTitle, GC.GeneralAbr, GC.GeneralTitleCode, GC.SystemName
	FROM VW_GeneralCoding GC
	WHERE GeneralId = @GeneralId
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_GeneralsUsedInActiveLedgers]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_GeneralsUsedInActiveLedgers](@SystemId int) AS
BEGIN
	SELECT DISTINCT
		GeneralId, SystemId, GeneralCode, GeneralTitle, GeneralAbr, SystemName,
		GeneralTitle +'('+ Cast(GeneralCode as nvarchar(50))+')' as GeneralTitleCode
	FROM	VW_GeneralActiveLedgers
	WHERE (@SystemId =0 OR SystemId = @SystemId)  
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_GeneralTitle]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_GeneralTitle]( @AbrList nvarchar(max) ) AS
BEGIN
	SELECT GeneralTitle as Title,GeneralAbr as Abr 
	FROM Generals G
	INNER JOIN dbo.FN_StringToTable_Not_NULL(@AbrList,',') B ON G.GeneralAbr = B.Item
END

GO

/****** Object:  StoredProcedure [dbo].[USP_Select_ImportExcelSchema_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_ImportExcelSchema_ById]( @ImportSchemaId int ) AS
BEGIN

	SELECT  
		ISC.ImportSchemaId, ISC.LedgerId, ISC.UNC, ISC.FolderName,ISC.SheetName,
		GeneralId,GeneralCode,GeneralTitle,LedgerCode,LedgerTitle, ISC.UNC+'\'+ISC.FolderName+'\' as FullPath
	FROM ImportSchemas ISC
	INNER JOIN VW_GeneralLedgerCoding GL ON ISC.LedgerId = GL.LedgerId
	WHERE (@ImportSchemaId =0 OR ImportSchemaId= @ImportSchemaId)

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_ImportScheduleByFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_ImportScheduleByFormula]( @FormulaId INT , @UserId int) AS
BEGIN
	--DECLARE @UserId int = 0, @FormulaId INT =103377 ;

	SELECT 
		ImportScheduleId,UserId,FormulaId,Title,FromDateM,FromDateS,ToDateM,ToDateS,ImportMode,ImportStatus
		,CASE ImportMode WHEN 0 THEN N'فرمول جاري' ELSE N'فرمول در بازه' END as ImportModeTitle
		,CASE ImportStatus WHEN 0 THEN N'انتظار' WHEN 1 THEN N'انجام شده' ELSE N'ناقص'END as ImportStatusTitle

	FROM ImportSchedule
	WHERE ( @FormulaId =0 OR FormulaId = @FormulaId) AND ( @UserId =0 OR UserId = @UserId )

END;




GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Ledger_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Ledger_ById]( @LedgerId int ) AS
BEGIN
	SELECT 
		L.LedgerId, L.GeneralId, L.LedgerCode, L.LedgerTitle, L.LedgerAbr, L.LedgerTotal, 
		L.GeneralCode, L.GeneralTitle, L.GeneralAbr, L.TotalTitle, L.SystemId, L.SystemName
	FROM  VW_GeneralLedgerCoding L
	WHERE LedgerId = @LedgerId
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LedgerCodes]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LedgerCodes]( @GeneralId int ) AS
BEGIN
	SELECT
		LedgerId, GeneralId, SystemId, LedgerCode, LedgerTitle, LedgerAbr, LedgerTotal,
		TotalTitle, GeneralCode, GeneralTitle, GeneralAbr, SystemName,
		'TotalTitleCode'= CONCAT(TotalTitle,'(',LedgerCode,')')
	FROM	VW_GeneralLedgerCoding
	WHERE (@GeneralId =0 OR GeneralId = @GeneralId)
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LedgerCodes_ForActiveLedgers]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LedgerCodes_ForActiveLedgers]( @GeneralId int ) AS
BEGIN
	SELECT
		LedgerId, GeneralId, SystemId, SystemName,  LedgerCode, LedgerTitle, LedgerAbr, LedgerTotal,
		TotalTitle, GeneralCode, GeneralTitle, GeneralAbr, SystemName,
		'TotalTitleCode'= CONCAT(SystemName,'-',TotalTitle,'(',LedgerCode,')')
	FROM	VW_GeneralLedgerCoding
	WHERE (@GeneralId =0 OR GeneralId = @GeneralId) AND LedgerId NOT IN(Select LedgerId from ActiveLedgers)
	ORDER BY SystemId, GeneralCode
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LedgersUsedInActiveLedgers]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LedgersUsedInActiveLedgers]( @GeneralId int ) AS
BEGIN
	SELECT
		LedgerId, GeneralId, SystemId, LedgerCode, LedgerTitle, LedgerAbr, LedgerTotal,
		TotalTitle, GeneralCode, GeneralTitle, GeneralAbr, SystemName,
		'TotalTitleCode'= CONCAT(TotalTitle,'(',LedgerCode,')')
	FROM	VW_GeneralActiveLedgers
	WHERE (@GeneralId =0 OR GeneralId = @GeneralId)
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LedgerTitle]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LedgerTitle]( @AbrList nvarchar(max) ) AS
BEGIN
	SELECT LedgerTitle as Title,LedgerTotal as Abr 
	FROM Ledgers L
	INNER JOIN dbo.FN_StringToTable_Not_NULL(@AbrList,',') B ON L.LedgerTotal = B.Item
END
GO

/****** Object:  StoredProcedure [dbo].[USP_Select_ListOfFormulas]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_ListOfFormulas]( @UserId int ) AS
BEGIN
	SELECT TOP 1000000
		FormulaId,SeqNO,Name,Code,
		Code+'('+Name +')' as FormulaTitle
	FROM Formulas
	ORDER BY SeqNO
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Log_ExcelImport]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Log_ExcelImport](@ShamsiFrom as char(10),@ShamsiTo as char(10) )AS
BEGIN
	SELECT TOP 1000000
		Log_ExcelImportId,TD.Shamsi as RunDateShamsi, Convert(Nvarchar(50),RunDate,108) as RunTime, FileNamePath, SheetName, FetchDateS, 
		LEI.LedgerId, GLC.LedgerCode, GLC.LedgerTitleCode, GLC.LedgerTotal,
		 Status,
		'StatusTitle'=Case Status when 0 then N'موفق' else N'ناموفق' end

	FROM Log_ExcelImport LEI
	LEFT JOIN VW_GeneralLedgerCoding GLC ON LEI.LedgerId = GLC.LedgerId
	LEFT JOIN TblDate TD ON TD.MiladiDate = CAST(RunDate as Date) 
	WHERE Td.Shamsi Between @ShamsiFrom and @ShamsiTo
	ORDER BY RunDate DESC, 3 desc  
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LogFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LogFormula]( @FormulaId as int ) as
BEGIN	 
--DECLARE @FormulaId as int = 104069;
	SELECT 
	GRP=  DENSE_RANK() OVER(   ORDER BY FM.ActionDate )  ,
	'FormulaType'=
		CASE FormulaType
			WHEN 0 THEN N'شامل معين هاي ...' 
			WHEN 1 THEN N'معين هاي از ... تا ...' 
			WHEN 2 THEN N'همه معين هاي کل ...' 
			WHEN 3 THEN N'دستي' 
		END,
	'ActType'=
		CASE ActType
			WHEN 'Deleted'   THEN N'حذف'
			WHEN 'Inserted'  THEN N'درج'
			WHEN 'UDeleted' THEN N'ويرايش از'
			WHEN 'UInserted'  THEN N'ويرايش به'
		END,
		FM.ActionDate,
		FM.FormulaId,
		US.UserName,
		FM.SeqNO,
		FM.Name,
		FM.Code,
		FM.Context, 
		GC.GeneralTitleCodeSystem,
		GLCF.TotalTitle as FromLedgerTitle , 
		GLCT.TotalTitle as ToLedgerTitle , 
		FM.IncludeLedgers,
		FM.CreateDateS,
		FM.ModifyDateS,
		FM.IsPrivate,
		CAST(FM.BranchesNameList as nvarchar(4000)) as BranchesNameList
	FROM LOG_Formulas FM
	LEFT JOIN Users US ON FM.UserId = US.UserId
	LEFT JOIN VW_GeneralCoding GC ON GC.GeneralId = FM.GeneralId
	LEFT JOIN VW_GeneralLedgerCoding GLCF ON GLCF.LedgerId = FM.FromLedger
	LEFT JOIN VW_GeneralLedgerCoding GLCT ON GLCT.LedgerId = FM.ToLedger
	WHERE FormulaId = @FormulaId and ActType <> 'UDeleted'
	ORDER BY FormulaId,LogId,FM.ActType desc

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_LogFormulasALL]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_LogFormulasALL]( @FormulaId int, @userId int ,@FromDateM Date='1921-01-01', @ToDateM Date='2999-01-01') AS
BEGIN
	--Declare @userId int =3 , @FormulaId int =0, @FromDateM Date='2014-10-06', @ToDateM Date='2014-10-07';
	WITH CTE AS
	(
		SELECT FormulaId, UserId FROM dbo.FN_Select_Formulas_ByAccessibility(@userId)  
	)
	SELECT 
	GRP=  DENSE_RANK() OVER( PARTITION BY FM.FormulaId  ORDER BY FM.ActionDate )  ,
	'FormulaType'=
		CASE FormulaType
			WHEN 0 THEN N'شامل معين هاي ...' 
			WHEN 1 THEN N'معين هاي از ... تا ...' 
			WHEN 2 THEN N'همه معين هاي کل ...' 
			WHEN 3 THEN N'دستي' 
		END,
	'ActType'=
		CASE ActType
			WHEN 'Deleted'   THEN N'حذف'
			WHEN 'Inserted'  THEN N'درج'
			WHEN 'UDeleted' THEN N'ويرايش از'
			WHEN 'UInserted'  THEN N'ويرايش به'
		END,
		FM.ActionDate,
		DT.Shamsi+Convert( nvarchar(20),FM.ActionDate,108)  ActionDateS,
		FM.FormulaId,
		US.UserName,
		FM.SeqNO,
		FM.Name,
		FM.Code,
		FM.Context, 
		GC.GeneralTitleCodeSystem,
		GLCF.TotalTitle as FromLedgerTitle , 
		GLCT.TotalTitle as ToLedgerTitle , 
		FM.IncludeLedgers,
		FM.CreateDateS,
		FM.ModifyDateS,
		FM.IsPrivate,
		Case ISNULL(FM.IsPrivate,0) WHEN 0 THEN NULL ELSE N'دارد' END  PrivateDesc,
		CAST(FM.BranchesNameList as nvarchar(4000)) as BranchesNameList
	FROM LOG_Formulas FM
	LEFT JOIN Users US ON FM.UserId = US.UserId
	LEFT JOIN VW_GeneralCoding GC ON GC.GeneralId = FM.GeneralId
	LEFT JOIN VW_GeneralLedgerCoding GLCF ON GLCF.LedgerId = FM.FromLedger
	LEFT JOIN VW_GeneralLedgerCoding GLCT ON GLCT.LedgerId = FM.ToLedger
	INNER JOIN CTE A ON A.FormulaId = FM.FormulaId	
	LEFT JOIN TblDate DT ON DT.MiladiDate = CAST(FM.ActionDate as DATE)
	WHERE ActType <> 'UDeleted' and ( FM.FormulaId = @FormulaId OR @FormulaId =0 ) and CAST( ActionDate as DATE) BETWEEN @FromDateM and @ToDateM
	ORDER BY FormulaId,LogId,FM.ActType desc
END;



GO

/****** Object:  StoredProcedure [dbo].[USP_Select_NewLedgers_Status]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_NewLedgers_Status](  @NewLedgerId int, @Status tinyint ) AS
BEGIN
	Select 
		NewLedgerId,CreateDate,AcceptDate,RejectDate,Status, 
		NL.LedgerCode, NL.GeneralCode, NL.SystemId,
		GL.LedgerId, GL.GeneralId
	FROM NewLedgers NL
	INNER JOIN VW_GeneralLedgerCoding  GL ON GL.LedgerCode = NL.LedgerCode AND GL.GeneralCode = NL.GeneralCode and GL.SystemId = NL.SystemId
	WHERE (@NewLedgerId = 0 OR NewLedgerId= @NewLedgerId) AND (@Status=0 OR Status=@Status)
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Reports]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Reports]( @Status Int ) AS
BEGIN
	SELECT  
		ROW_NUMBER() OVER ( ORDER BY ReportId) as RowNo,
		ReportId, ActionDateS,  FetchDateS,  Status, MsgError, ReTry, Backward,
		CASE STATUS WHEN 1 THEN NULL ELSE N'ناموفق' END as  StatusDesc 
	FROM Reports
	WHERE (@Status =0 OR Status = @Status)
END








GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Similar_Formulas]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Similar_Formulas]( @SeqNO int ) AS
BEGIN 
	DECLARE @Ret nvarchar(max) ='';

	SELECT 
		 @Ret = CONCAT( @Ret,CASE @Ret WHEN '' THEN '' ELSE  ',' END, F.Code  )
	FROM VW_Select_Similar_Formulas SF
	INNER JOIN Formulas F ON SF.SeqNO = F.SeqNO
	WHERE GRP = ( SELECT GRP FROM  VW_Select_Similar_Formulas Where SeqNO = @SeqNO )
	SELECT NULLIF( LTRIM(RTRIM(@Ret)),'') as Codes
END  









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Similar_Formulas_ByContext]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Similar_Formulas_ByContext]( @Context nvarchar(max), @MySeqNO int ) AS
BEGIN
	--DECLARE @Context nvarchar(max)='[GL_4350_2_1]', @MySeqNO int=82 ;
	--DECLARE @Context nvarchar(max)='[GL_4112_2_1]', @MySeqNO int=12 ;

	DECLARE @Ret nvarchar(max) ='';

	WITH CTEFormulas AS
	(
		SELECT  SEQNO, Context	FROM Formulas F Where SeqNO <> @MySeqNO
		UNION
		SELECT 0, @Context
	)
	,CTEPre AS
	( 
		SELECT  SEQNO, B.Item  as GL_Code	FROM CTEFormulas F 
		CROSS APPLY dbo.FN_StringToTableFormula(Context) B
	)
	,CTEResult AS
	(
		SELECT  DISTINCT A.SeqNO,B.Codes FROM CTEPre A
		CROSS APPLY( SELECT GL_CODE+',' FROM CTEPre WHERE SeqNO = A.SeqNO FOR XML PATH('')) B(Codes)
	)
	,CTEFinal AS
	(
		SELECT 
			'GRP' = DENSE_RANK() OVER( ORDER BY Codes ),SeqNO,Codes 
		FROM CTEResult 
		WHERE Codes IN (SELECT Codes FROM CTEResult GROUP BY Codes HAVING COUNT(*)>1 )
	)
	SELECT 
		 @Ret = CONCAT( @Ret,CASE @Ret WHEN '' THEN '' ELSE  ',' END, F.Code  )
	FROM CTEFinal SF
	INNER JOIN Formulas F ON SF.SeqNO = F.SeqNO
	WHERE GRP = ( SELECT GRP FROM  CTEFinal Where SeqNO = 0 ) AND SF.SeqNO <> 0

	SELECT NULLIF( LTRIM(RTRIM(@Ret)),'') as Codes
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_UsedInFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_UsedInFormula]( @FormulaId int )  AS
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
	)
	,CTE_ALL AS
	(
		SELECT 
			SeqNo,Code,Code2, A.Item Lst
		FROM CTE_TEMP B
		CROSS APPLY dbo.FN_StringToTable_Not_NULL( UsedLedgersAbr2 ,',') A
	)
	,CTE_RETUEN AS
	(
	SELECT DISTINCT B.Code2 
	FROM CTE_ALL A
	INNER JOIN  (SELECT SeqNo,Code2, Lst FROM CTE_ALL WHERE Lst = @Code) B	ON B.Lst = A.Code2
	)
	SELECT @Ret= @Ret+ CASE @Ret WHEN '' THEN '' ELSE ' , ' END+Code2 FROM CTE_RETUEN;

	SELECT NULLIF(@RET,'') As UsedFormulaList
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_UserAccesses_ByUserId]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_UserAccesses_ByUserId]( @UserId Int ) AS
BEGIN
	SELECT  
		A.AccessId, A.AccessCode, A.Title ,
		UA.UserAccessId, UA.UserId 
	FROM Accesses A
	LEFT JOIN UserAccesses UA ON A.AccessId = UA.AccessId
	WHERE UserId = @UserId 
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_UserGroups_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_UserGroups_ById]( @UserGroupId as nvarchar(50)) AS
BEGIN
	Select UserGroupId, GroupName  from UserGroups
	WHERE UserGroupId=@UserGroupId
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_UserGroups_ByName]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_UserGroups_ByName]( @GroupName as nvarchar(50)) AS
BEGIN
	Select UserGroupId, GroupName  from UserGroups
	WHERE GroupName=@GroupName
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Users_ByGroupId]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Users_ByGroupId]( @UserGroupId as int) AS
BEGIN

	SELECT
		UserId, UserName, Title, UserPass, AccessMode , UG.UserGroupId, UG.GroupName,
		CASE AccessMode 
			WHEN 0 THEN N'مدير' 
			WHEN 1 THEN N'سرپرست' 
			WHEN 2 THEN N'کاربر' 
		END as AccessModeTitle
	FROM Users U
	LEFT JOIN UserGroups UG ON U.UserGroupId = UG.UserGroupId
	WHERE ( ISNULL(@UserGroupId,0) =0 OR U.UserGroupId= @UserGroupId )
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Users_ById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Users_ById]( @UserId as int=NULL) AS
BEGIN

	SELECT
		UserId, UserName, Title, UserPass, AccessMode , UG.UserGroupId, UG.GroupName,
		CASE AccessMode 
			WHEN 0 THEN N'مدير' 
			WHEN 1 THEN N'سرپرست' 
			WHEN 2 THEN N'کاربر' 
		END as AccessModeTitle
	FROM Users U
	LEFT JOIN userGroups UG ON U.UserGroupId = UG.UserGroupId
	WHERE ( ISNULL(@UserId,0) =0 OR UserId= @UserId )
END









GO

/****** Object:  StoredProcedure [dbo].[USP_Select_Users_ByName]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Select_Users_ByName]( @UserName as nvarchar(50) ) AS
BEGIN

	SELECT
		UserId, UserName, Title, UserPass, AccessMode
	FROM Users
	WHERE UserName = @UserName
END









GO

/****** Object:  StoredProcedure [dbo].[USP_SelectBranches]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectBranches]( @BranchId int) AS
BEGIN
	Select BranchId,Parent,BranchName,BranchNameTotal,BranchNameTotalDesc from VW_BranchesAll
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SelectMultiCodeResult]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectMultiCodeResult]( @FetchDateM Date , @Codes nvarchar(4000) ) AS
BEGIN
	--DECLARE @FetchDateM Date = '2015-10-03', @Codes nvarchar(4000) ='Deb15,Deb70,Deb72,Deb19,Deb1';
	
	DECLARE @Script nvarchar(max) ='';
	IF LTRIM(RTRIM(@Codes)) ='' SET @Codes = N'EmptyList';
	SET  @Script=
		'
		SELECT BR.BranchId as N''کد شعبه'', BR.BranchName as N''شعبه'','+@Codes+Char(13)+
		'FROM
		(
			SELECT BranchId,'+@Codes+Char(13)+
			'FROM 
			( 
				SELECT BranchId, Code, Remain 
				FROM ResultFormulas 
				WHERE FetchDateM = '''+ Cast(@FetchDateM as varchar(10))+''' and code in ( '''+Replace(@Codes,',',''',''')+''' )
			) as TBL 
			PIVOT(	SUM(Remain) FOR Code IN	( '+@Codes+')	) PVT
		)K
		INNER JOIN Branches BR ON BR.BranchId = K.BranchId
		';
	EXEC ( @Script )

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SelectMutiFormulaTreeDrivedTo]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectMutiFormulaTreeDrivedTo]( @FormulaCodes nvarchar(max) ) AS 
BEGIN
	--Declare @FormulaCodes nvarchar(max)='[Loan64],[Hamrah407],[Deb8],[Deb12]';

	DECLARE @CTE TABLE (FormulaType int, Code varchar(50), SeqNO int, UsedCode varchar(100));
	INSERT @CTE SELECT FormulaType,Code, SeqNO, UsedCode FROM VW_SeperatedItemFormulas  

	;WITH CTE_REC AS
	(
		SELECT 0 as LV ,FormulaType,Code,SeqNO,UsedCode FROM @CTE  
		WHERE Code in (Select Item From dbo.FN_StringToTable_Not_NULL( @FormulaCodes, ',') )
		UNION ALL
		SELECT B.LV+1, A.FormulaType,A.Code,A.SeqNO, A.UsedCode FROM @CTE A
		INNER JOIN CTE_REC B ON B.Code = A.UsedCode   
	)
	SELECT 
	DISTINCT
		A.Code, A.SeqNO ,IIF( A.FormulaType = 3, F.Context, NULL ) as Context
	FROM CTE_REC A
	INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
	ORDER BY A.SeqNO
END



GO

/****** Object:  StoredProcedure [dbo].[USP_SelectResultTrandByGeneral]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectResultTrandByGeneral]( @FetchDateM Date , @AbrList nvarchar(Max), @Value nvarchar(15) ) AS
BEGIN
	--DECLARE @FetchDateM Date = '2014-08-03', @AbrList nvarchar(max)='G_404_3,G_407_3,G_409_3,G_411_3', @Value nvarchar(15)='LastRemain';
	
	DECLARE @Script nvarchar(max) ='';
	IF LTRIM(RTRIM(@AbrList)) ='' SET @AbrList = N'EmptyList';
	SET  @Script=
		'SELECT BR.BranchId as N''کد شعبه'', BR.BranchName as N''شعبه'','+@AbrList+Char(13)+
		'FROM
		(
			SELECT BranchId,'+@AbrList+Char(13)+
			'FROM 
			( 
				SELECT BranchId, GeneralAbr,'+@Value+' 
				FROM ResultTrans RT
				INNER JOIN VW_GeneralLedgerCoding GLC ON RT.LedgerId = GLC.LedgerId 
				WHERE FetchDateM = '''+ Cast(@FetchDateM as varchar(10))+''' and GeneralAbr in ( '''+Replace(@AbrList,',',''',''')+''' )
			) as TBL 
			PIVOT(	SUM('+@Value+') FOR GeneralAbr IN	( '+@AbrList+')	) PVT
		)K
		INNER JOIN Branches BR ON BR.BranchId = K.BranchId
		';
	EXEC ( @Script )

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SelectResultTrandByGeneralRange]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectResultTrandByGeneralRange]( @FetchDateMFrom Date, @FetchDateMTo Date, @AbrList nvarchar(Max), @Value nvarchar(15) ) WITH RECOMPILE AS
BEGIN
	--DECLARE @FetchDateMFrom Date = '2014-07-03', @FetchDateMTo Date = '2014-07-06', @AbrList nvarchar(max)='G_404_3,G_407_3,G_409_3,G_411_3', @Value nvarchar(15)='DebitValue';
	
	DECLARE @Script nvarchar(max) ='', @ValueFiled nvarchar(1000);
	SELECT @ValueFiled = 
			CASE @Value 
				WHEN 'FirstRemain'	THEN 'FirstRemain = FIRST_VALUE(SUM(FirstRemain)) OVER( PARTITION BY BranchId,GeneralAbr ORDER BY FetchdateM ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
				WHEN 'LastRemain'	THEN 'LastRemain  = LAST_VALUE(SUM(LastRemain)) OVER( PARTITION BY BranchId,GeneralAbr ORDER BY FetchdateM ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
				WHEN 'CreditValue'	THEN 'CreditValue'
				WHEN 'DebitValue'	THEN 'DebitValue'
			END;
	

	IF LTRIM(RTRIM(@AbrList)) ='' SET @AbrList = N'EmptyList';
	SET  @Script=
		'SELECT BR.BranchId as N''کد شعبه'', BR.BranchName as N''شعبه'','+@AbrList+Char(13)+
		'FROM
		(
			SELECT BranchId,'+@AbrList+Char(13)+
			'FROM 
			( 
				SELECT DISTINCT BranchId, GeneralAbr,'+@ValueFiled+' 
				FROM ResultTrans RT
				INNER JOIN VW_GeneralLedgerCoding GLC ON RT.LedgerId = GLC.LedgerId 
				WHERE FetchDateM Between '''+ Cast(@FetchDateMFrom as varchar(10))+''' and '''+ Cast(@FetchDateMTo as varchar(10))+ ''' and GeneralAbr in ( '''+Replace(@AbrList,',',''',''')+''' )
				'+
				CASE WHEN @Value in ('FirstRemain', 'LastRemain') THEN 'GROUP BY BranchId, GeneralAbr, FetchDateM' ELSE '' END+'
			) as TBL 
			PIVOT(	SUM('+@Value+') FOR GeneralAbr IN	( '+@AbrList+')	) PVT
		)K
		INNER JOIN Branches BR ON BR.BranchId = K.BranchId
		';
	EXEC ( @Script )
END;




GO

/****** Object:  StoredProcedure [dbo].[USP_SelectResultTrandByLedgers]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectResultTrandByLedgers]( @FetchDateM Date , @AbrList nvarchar(Max), @Value nvarchar(15) ) AS
BEGIN
	--DECLARE @FetchDateM Date = '2014-08-03', @AbrList nvarchar(max)='GL_1230_2_10,GL_1230_2_52,GL_1230_2_8,GL_1300_2_6,GL_1500_2_17,GL_1500_2_179,GL_4280_2_53', @Value nvarchar(15)='LastRemain';
	
	DECLARE @Script nvarchar(max) ='';
	IF LTRIM(RTRIM(@AbrList)) ='' SET @AbrList = N'EmptyList';
	SET  @Script=
		'SELECT BR.BranchId as N''کد شعبه'', BR.BranchName as N''شعبه'','+@AbrList+Char(13)+
		'FROM
		(
			SELECT BranchId,'+@AbrList+Char(13)+
			'FROM 
			( 
				SELECT BranchId, LedgerTotal,'+@Value+' 
				FROM ResultTrans RT
				INNER JOIN VW_GeneralLedgerCoding GLC ON RT.LedgerId = GLC.LedgerId 
				WHERE FetchDateM = '''+ Cast(@FetchDateM as varchar(10))+''' and LedgerTotal in ( '''+Replace(@AbrList,',',''',''')+''' )
			) as TBL 
			PIVOT(	SUM('+@Value+') FOR LedgerTotal IN	( '+@AbrList+')	) PVT
		)K
		INNER JOIN Branches BR ON BR.BranchId = K.BranchId
		';
	EXEC ( @Script )

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SelectResultTrandByLedgersRange]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SelectResultTrandByLedgersRange](@FetchDateMFrom Date, @FetchDateMTo Date, @AbrList nvarchar(Max), @Value nvarchar(15) ) WITH RECOMPILE AS
BEGIN
	
	--DECLARE @FetchDateMFrom Date = '2014-07-03', @FetchDateMTo Date = '2014-07-06', @AbrList nvarchar(max)='GL_1375_2_275,GL_1376_2_273,GL_1385_2_176,GL_1460_2_176,GL_4690_2_176', @Value nvarchar(15)='CreditValue';
	
	DECLARE @Script nvarchar(max) ='', @ValueFiled nvarchar(1000);
	SELECT @ValueFiled = 
			CASE @Value 
				WHEN 'FirstRemain'	THEN 'FirstRemain = FIRST_VALUE(FirstRemain) OVER( PARTITION BY BranchId,LedgerTotal ORDER BY FetchdateM ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
				WHEN 'LastRemain'	THEN 'LastRemain  = LAST_VALUE(LastRemain) OVER( PARTITION BY BranchId,LedgerTotal ORDER BY FetchdateM ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
				WHEN 'CreditValue'	THEN 'CreditValue'
				WHEN 'DebitValue'	THEN 'DebitValue'
			END;

	IF LTRIM(RTRIM(@AbrList)) ='' SET @AbrList = N'EmptyList';
	SET  @Script=
		'SELECT BR.BranchId as N''کد شعبه'', BR.BranchName as N''شعبه'','+@AbrList+Char(13)+
		'FROM
		(
			SELECT BranchId,'+@AbrList+Char(13)+
			'FROM 
			( 
				SELECT BranchId, LedgerTotal,'+@ValueFiled+' 
				FROM ResultTrans RT
				INNER JOIN VW_GeneralLedgerCoding GLC ON RT.LedgerId = GLC.LedgerId 
				WHERE FetchDateM Between '''+ Cast(@FetchDateMFrom as varchar(10))+''' and '''+ Cast(@FetchDateMTo as varchar(10))+ ''' and LedgerTotal in ( '''+Replace(@AbrList,',',''',''')+''' )
			) as TBL 
			PIVOT(	SUM('+@Value+') FOR LedgerTotal IN	( '+@AbrList+')	) PVT
		)K
		INNER JOIN Branches BR ON BR.BranchId = K.BranchId
		';
	EXEC ( @Script )

END;


GO

/****** Object:  StoredProcedure [dbo].[USP_Seperate_CompressedFormulaById]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Seperate_CompressedFormulaById]
			( 
			@FormulaId Int,
			@LedgerAbrList nvarchar(max) OUT,
			@LedgerCodeList nvarchar(max) OUT,
			@LedgerIdList nvarchar(max) OUT, 
			@Formula nvarchar(max) OUT, 
			@LedgerAbrListISNULL nvarchar(max) OUT,
			@BranchesIdList nvarchar(max) OUT
			) AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY DROP TABLE #TestValidityFormula END TRY BEGIN CATCH END CATCH 

	CREATE	TABLE #TestValidityFormula(
					LedgerAbrList nvarchar(max) ,
					LedgerCodeList nvarchar(max) ,
					LedgerIdList nvarchar(max) , 
					Formula nvarchar(max),
					LedgerTitleList nvarchar(max),
					ErrorList nvarchar(max) , 
					HasError Int
					);
	DECLARE @CompressedFormula nvarchar(Max)='', @ErrorList nvarchar(max),@HasError  nvarchar(max);
	SELECT	@CompressedFormula = CompressedFormula, @BranchesIdList = NULLIF(LTRIM(RTRIM(BranchesIdList)),'')	FROM Formulas	WHERE FormulaId = @FormulaId;

	INSERT INTO #TestValidityFormula	EXEC USP_TestValidityFormula @CompressedFormula;
	
	SELECT 
		@LedgerAbrList = LedgerAbrList,
		@LedgerCodeList = LedgerCodeList,
		@LedgerIdList = LedgerIdList,
		@Formula = Formula,
		@ErrorList = ErrorList,
		@HasError = HasError
	FROM #TestValidityFormula;
	
	IF @LedgerCodeList IS NOT NULL
		SELECT 
			@LedgerAbrListISNULL=CONCAT(@LedgerAbrListISNULL,CASE ISNULL(@LedgerAbrListISNULL,'') WHEN '' THEN '' ELSE ',' END ,'ISNULL(',Item,',0)')
		FROM dbo.FN_StringToTable( @LedgerCodeList, ',' )
	
END



















GO

/****** Object:  StoredProcedure [dbo].[USP_Set_Status_Reports]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Set_Status_Reports]( @StatusList nvarchar(max) ,@ReTry TinyInt) AS
BEGIN
  
	UPDATE Reports
	SET ReTry = @ReTry
	FROM dbo.FN_StringToTable_Not_NULL(@StatusList,',')
	WHERE ReportId= Item
		
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SetPrivateFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SetPrivateFormula]( @FormulaId int, @ISPrivate tinyint )  AS
BEGIN
	UPDATE Formulas
		SET	ISPrivate=@ISPrivate
	WHERE FormulaId= @FormulaId;
	WITH CTE AS
	(
		SELECT TOP 1
			FormulaId, ISPrivate 
		FROM LOG_Formulas WHERE FormulaId= @FormulaId AND ActType LIKE '%Inserted%'
		ORDER BY ActionDate DESC 
	)
	UPDATE CTE  
		SET ISPrivate = @ISPrivate;
	RETURN 0;
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_CreateLinkServer]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_CreateLinkServer]( @Server_Name nvarchar(100), @Data_Source  nvarchar(100), @Remote_User  nvarchar(100), @Remote_Password  nvarchar(100) ) AS
BEGIN 
	--@Server_Name =N'REFAHSERVER'; @Data_Source= N'REIHANE-PC'
	-- USP_SYS_CreateLinkServer N'REFAHSERVER', N'REIHANE-PC', 'sa', '123456' ;
	EXEC master.dbo.sp_addlinkedserver @server = @Server_Name, @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=@Data_Source
	EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=@Server_Name, @useself=N'False', @locallogin=NULL, @rmtuser=@Remote_User, @rmtpassword=@Remote_Password
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'data access', @optvalue=N'true'
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'rpc', @optvalue=N'true'
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'rpc out', @optvalue=N'true'
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'connect timeout', @optvalue=N'0'
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'use remote collation', @optvalue=N'true'
	EXEC master.dbo.sp_serveroption @server=@Server_Name, @optname=N'remote proc transaction promotion', @optvalue=N'true'
	EXEC sp_serveroption @Server_Name, 'remote proc transaction promotion', 'false';

/*
USE BSC;
GO
BEGIN TRY DROP TABLE #TEMP; END TRY BEGIN CATCH END CATCH;
BEGIN TRY DROP SYNONYM SYN_GetBalanceScore; END TRY BEGIN CATCH END CATCH;

ALTER SYNONYM SYN_GetBalanceScore FOR REFAHSERVER.Test.dbo.USP_GetBalanceScore;
ALTER TABLE #TEMP(
	[siFiles] [int] NOT NULL,
	[FileID] [int] NULL,
	[FName] [varchar](15) NULL,
	[LName] [varchar](25) NULL,
	[Phone1] [varchar](20) NULL,
	[Phone2] [varchar](20) NULL,
	[Sex] [int] NULL,
	[BirthYear] [varchar](4) NULL,
	[ReferDate] [varchar](10) NULL,
	[Subject] [varchar](200) NULL,
	[Comment] [varchar](4000) NULL
) ON [PRIMARY];

INSERT INTO #TEMP	EXEC SYN_GetBalanceScore 5


SELECT * FROM #TEMP;

DROP TABLE #TEMP
-- ALTER SYNONYM SYN_GetBalanceScore FOR REFAHSERVER.Test.dbo.USP_GetBalanceScore
-- DROP SYNONYM SYN_GetBalanceScore
--Use Master;
--sp_dropserver 'REFAHSERVER', 'droplogins';

*/
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_GetProcInfo]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_GetProcInfo]( @ProcName varchar(4000) ) AS
BEGIN
	CREATE	TABLE #TempProcInfo
	(
	PROCEDURE_QUALIFIER nvarchar(4000),
	PROCEDURE_OWNER nvarchar(4000),
	PROCEDURE_NAME nvarchar(4000),
	COLUMN_NAME nvarchar(4000),
	COLUMN_TYPE smallint,
	DATA_TYPE smallint,
	[TYPE_NAME] nvarchar(4000),
	[PRECISION] int,
	LENGTH int,
	SCALE smallint,
	RADIX smallint,
	NULLABLE smallint,
	REMARKS nvarchar(4000),
	COLUMN_DEF nvarchar(4000),
	SQL_DATA_TYPE smallint,
	SQL_DATETIME_SUB smallint,
	CHAR_OCTET_LENGTH int,
	ORDINAL_POSITION int,
	IS_NULLABLE nvarchar(254),
	SS_DATA_TYPE tinyint
	)

	INSERT INTO #TempProcInfo EXEC sp_sproc_columns @procedure_name = @procname
	SELECT
	'ParamName' = COLUMN_NAME ,
	'ParamType' = COLUMN_TYPE ,
	[Type_name] ,
	[Precision] ,
	Length ,
	Scale
	FROM #TempProcInfo

	DROP TABLE #TempProcInfo
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_GetProcInfo_NO_AT]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_GetProcInfo_NO_AT]( @ProcName varchar(4000) ) AS  
BEGIN  
 CREATE	TABLE #TempProcInfo1   
 (  
 PROCEDURE_QUALIFIER nvarchar(4000),  
 PROCEDURE_OWNER nvarchar(4000),  
 PROCEDURE_NAME nvarchar(4000),  
 COLUMN_NAME nvarchar(4000),  
 COLUMN_TYPE smallint,  
 DATA_TYPE smallint,  
 [TYPE_NAME] nvarchar(4000),  
 [PRECISION] int,  
 LENGTH int,  
 SCALE smallint,  
 RADIX smallint,  
 NULLABLE smallint,  
 REMARKS nvarchar(4000),  
 COLUMN_DEF nvarchar(4000),  
 SQL_DATA_TYPE smallint,  
 SQL_DATETIME_SUB smallint,  
 CHAR_OCTET_LENGTH int,  
 ORDINAL_POSITION int,  
 IS_NULLABLE nvarchar(254),  
 SS_DATA_TYPE tinyint  
 )  
  
 INSERT INTO #TempProcInfo1 EXEC sp_sproc_columns @procedure_name = @procname  
 SELECT  
 'ParamName' = Right(COLUMN_NAME,Len(COLUMN_NAME)-1) ,  
 'ParamType' = COLUMN_TYPE ,  
 [Type_name] ,  
 [Precision] ,  
 Length ,  
 Scale  
 FROM #TempProcInfo1  
  
 DROP TABLE #TempProcInfo1  
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_Modify_LogFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_Modify_LogFormula] AS
BEGIN
	INSERT INTO LOG_Formulas
	(ActionDate,LogId,ActType,FormulaId,UserId,SeqNO,FormulaType,Name,Code,MethodDesc,Context,GeneralId,FromLedger,ToLedger,IncludeLedgers,CreateDateM,CreateDateS,ModifyDateM,ModifyDateS,Comment,BranchesIdList,BranchesNameList,BranchTotalList,WhereClause,UsedLedgers,UsedLedgersAbr,CompressedFormula,IsPrivate,OwnGUID)
	SELECT 
		CreateDateM,(NEXT VALUE FOR dbo.LogSequence),'Inserted',FormulaId,UserId,SeqNO,FormulaType,Name,Code,MethodDesc,Context,GeneralId,FromLedger,ToLedger,IncludeLedgers,CreateDateM,CreateDateS,NULL,NULL,Comment,BranchesIdList,BranchesNameList,BranchTotalList,WhereClause,UsedLedgers,UsedLedgersAbr,CompressedFormula,IsPrivate,OwnGUID
	FROM Formulas F
	WHERE Code NOT IN ( SELECT Code FROM LOG_Formulas WHERE ActType in('Inserted','UInserted'))
END




GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_RoutineText]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_RoutineText]( @RoutineName as varchar(200)) AS
BEGIN
	DECLARE @RoutineText VARCHAR(MAX)='';
	SELECT @RoutineText = CONCAT(@RoutineText, SC.text) 
	FROM Syscomments SC
	WHERE OBJECT_NAME(id)= @RoutineName
	PRINT @RoutineText
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_RoutineUsed]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_RoutineUsed]( @RoutineName as varchar(200)) AS
BEGIN
	SELECT SO.name, SC.Text 
	FROM Syscomments SC
	INNER JOIN Sysobjects SO ON SO.id = SC.id
	WHERE text LIKE '%'+@RoutineName+'%'
	Order by 1;
END;











GO

/****** Object:  StoredProcedure [dbo].[USP_SYS_TreeFormulaDrivedFromCode]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_SYS_TreeFormulaDrivedFromCode]( @Code as nvarchar(100) ) AS 
BEGIN

	DECLARE @CTE TABLE (FormulaType int, Code varchar(50), SeqNO int, UsedCode varchar(100));

	INSERT @CTE SELECT FormulaType, Code, SeqNO, UsedCode FROM VW_SeperatedItemFormulas;

	WITH CTE_REC AS
	(
		SELECT 0 as LV, FormulaType, Code, SeqNO, UsedCode FROM @CTE  
		WHERE FormulaType = 3  and Code = @Code 
		UNION ALL
		SELECT B.LV+1, A.FormulaType, A.Code, A.SeqNO, A.UsedCode FROM @CTE A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode 
	)
	SELECT 
		DISTINCT
		LV, 
		A.FormulaType, 
		A.Code, 
		A.SeqNO, 
		CASE A.FormulaType WHEN 3 THEN F.Context ELSE NULL END AS Context
	FROM CTE_REC A
	INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
	ORDER BY LV,A.SeqNO

END 

 






GO

/****** Object:  StoredProcedure [dbo].[USP_TableLockedList]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC	[dbo].[USP_TableLockedList] AS
BEGIN
	SELECT DISTINCT 
		object_name(PAR.object_id) ObjectName,request_session_id
	FROM   sys.dm_tran_locks	LCK
	INNER JOIN sys.partitions	PAR	ON LCK.resource_associated_entity_id = PAR.hobt_id
END

GO

/****** Object:  StoredProcedure [dbo].[USP_TestValidityFormula]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_TestValidityFormula]( @Formula nvarchar(max) ) AS
BEGIN
	--DECLARE @Formula nvarchar(max) ='[F2]+[GL_4070_2_4]+101 '
	BEGIN TRY DROP TABLE #Temp			END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE #TempString	END TRY BEGIN CATCH END CATCH;

	DECLARE 
		@ListId nvarchar(max)='', 
		@ErrorList nvarchar(max)='', 
		@LedgerIdList nvarchar(max)='', 
		@LedgerCodeList nvarchar(max)='',
		@LedgerTitleList nvarchar(max)='';

	SELECT @Formula= REPLACE( REPLACE(REPLACE(@Formula,Char(10),''),Char(13),'') ,' ','');
	SELECT @Formula = dbo.FN_RemoveCommentsInFormula(@Formula, 0)
	IF @Formula ='<ERROR IN COMMENT>'
	BEGIN
		SELECT  NULL as LedgerAbrList, NULL as LedgerIdList, NULL as Formula,
				'<ERROR IN COMMENT>' as ErrorList,  2 as HasError ;
	END
	ELSE
	BEGIN
	 	;WITH Rows1 AS 
			( SELECT 1 as RowNO UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1  UNION ALL SELECT 1 )
		,RowCounter AS -- 271737 Rows 
			(SELECT ROW_NUMBER()OVER( ORDER BY (SELECT NULL))  as RowNO FROM Rows1 a, Rows1 b, Rows1 c, Rows1 d, Rows1 e, Rows1 f, Rows1 g) 
		, CTE AS
			(
			SELECT 
				C,S, RowNO, 
				LastBracket = LAST_VALUE(C) OVER( ORDER BY S RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
				ROW_NUMBER() OVER(ORDER BY RowNO DESC)  Last_Row,
				ROW_NUMBER() OVER(ORDER BY RowNO) RW, 
				LEAD(RowNO)  OVER(ORDER BY RowNO) RN,
				K = SUM(C2) OVER(  ORDER By ROWNO)
			FROM(
				SELECT 
					CAST(SUBSTRING(S,RowNO,1) as char(1)) as C,
					CASE CAST(SUBSTRING(S,RowNO,1) as char(1)) WHEN '[' THEN 1 WHEN ']' THEN -1 ELSE 100 END as C2,
					S,
					RowNO 
				FROM ( SELECT @Formula as S, Len(@Formula) as L ) Tbl
				INNER JOIN RowCounter ON RowNO <=L
				) A
			WHERE C in('[',']')
			)
		SELECT RW,C,K,S, LastBracket,Last_Row,RowNO,RN,SUBSTRING( S, RowNO, RN-RowNO+1 ) as CodeList,SUBSTRING( S, RowNO+1, LEN(S)-RowNO ) as ExtraCode
		INTO #Temp
		FROM CTE;
		
		SELECT RW,C,K,S,LastBracket,Last_Row,RowNO,RN,CodeList,ExtraCode INTO #TempString	FROM  #Temp	WHERE 	RW %2 = 1 ;
		
		DECLARE @LastBracket varchar(5)='', @ExtraCode nvarchar(50)='', @Test nvarchar(4000)='';		
		SELECT @LastBracket= LastBracket, @ExtraCode= ExtraCode FROM #Temp WHERE Last_Row=1;

		BEGIN TRY EXEC('Declare @A INT; SELECT @A= Col  FROM (SELECT  1'+@ExtraCode+' as Col) P') END TRY 
		BEGIN CATCH		SET @ErrorList=@ExtraCode	END CATCH

		IF	@ErrorList <> '' 
		BEGIN
			SET @ErrorList=@ExtraCode
		END
		ELSE
		IF EXISTS(SELECT 1 FROM #Temp WHERE K NOT BETWEEN 0 and 1) OR (@LastBracket <> ']')
		BEGIN
			SET @ErrorList ='Bracket Error';
		END
		ELSE
		BEGIN

			SELECT 
				@ListId = @ListId + Case @ListId when '' then '' else ',' end +	ISNULL(REPLACE( REPLACE( CodeList ,']','') ,'[',''),''),
				@LedgerCodeList = @LedgerCodeList + Case @LedgerCodeList when '' then '' else ',' end +	ISNULL(CodeList,''),
				@LedgerTitleList = @LedgerTitleList + Case @LedgerTitleList when '' then '' else ',' end +	ISNULL(LedgerTitle,''),
				@LedgerIdList = @LedgerIdList + Case @LedgerIdList when '' then '' else ',' end +	ISNULL(REPLACE( REPLACE( LedgerId ,']','') ,'[',''),'')
			FROM
			(
				SELECT DISTINCT
					CodeList, LedgerId, LedgerTitle 
				FROM #TempString A
				INNER JOIN VW_ActiveLedgerCodesAndFormula B ON A.CodeList = '['+B.LedgerTotal+']' 
			) K;

			SELECT 
				@ErrorList = @ErrorList + Case @ErrorList when '' then '' else Char(13) end + ISNULL(REPLACE( REPLACE( CodeList ,']','') ,'[',''),'')
			FROM #TempString
			WHERE SUBSTRING( S, RowNO, RN-RowNO+1 ) NOT IN (SELECT '['+LedgerTotal+']' FROM VW_ActiveLedgerCodesAndFormula);

		END;
		SELECT  NULLIF(@ListId,'') as LedgerAbrList, NULLIF(@LedgerCodeList,'') as LedgerCodeList, 
				NULLIF(@LedgerIdList,'') as LedgerIdList, NULLIF(@Formula,'') as Formula,
				NULLIF(@LedgerTitleList,'') as LedgerTitleList, 
				NULLIF(@ErrorList,'') as ErrorList, CASE WHEN NULLIF(@ErrorList,'') IS NULL THEN 0 ELSE 1 END HasError ;
	END;
	
	
	BEGIN TRY DROP TABLE #TempString	END TRY BEGIN CATCH END CATCH;
END;
GO

/****** Object:  StoredProcedure [dbo].[USP_TestValidityFormula_OldRowcounterError]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_TestValidityFormula_OldRowcounterError]( @Formula nvarchar(max) ) AS
BEGIN
	--DECLARE @Formula nvarchar(max) ='[F2]+[GL_4070_2_4]+101 '
	BEGIN TRY DROP TABLE #TempString	END TRY BEGIN CATCH END CATCH;

	DECLARE 
		@ListId nvarchar(max)='', 
		@ErrorList nvarchar(max)='', 
		@LedgerIdList nvarchar(max)='', 
		@LedgerCodeList nvarchar(max)='',
		@LedgerTitleList nvarchar(max)='';

	SELECT @Formula= REPLACE( REPLACE(REPLACE(@Formula,Char(10),''),Char(13),'') ,' ','');
	SELECT @Formula = dbo.FN_RemoveCommentsInFormula(@Formula, 0)
	IF @Formula ='<ERROR IN COMMENT>'
	BEGIN
		SELECT  NULL as LedgerAbrList, NULL as LedgerIdList, NULL as Formula,
				'<ERROR IN COMMENT>' as ErrorList,  2 as HasError ;
	END
	ELSE
	BEGIN
		SELECT S,RowNO,RN,SUBSTRING( S, RowNO, RN-RowNO+1 ) as CodeList
		INTO #TempString
		FROM(
			SELECT 
				S, RowNO,
				ROW_NUMBER() OVER(ORDER BY RowNO) RW, 
				LEAD(RowNO)  OVER(ORDER BY RowNO) RN
			FROM(
				SELECT SUBSTRING(S,RowNO,1) as C,S,RowNO FROM ( SELECT @Formula as S, Len(@Formula) as L ) Tbl
				INNER JOIN RowCounter ON RowNO <=L
				) A
			WHERE C in('[',']')
			)B
		WHERE 
			RW %2 = 1 ;

		SELECT 
			@ListId = @ListId + Case @ListId when '' then '' else ',' end +	ISNULL(REPLACE( REPLACE( CodeList ,']','') ,'[',''),''),
			@LedgerCodeList = @LedgerCodeList + Case @LedgerCodeList when '' then '' else ',' end +	ISNULL(CodeList,''),
			@LedgerTitleList = @LedgerTitleList + Case @LedgerTitleList when '' then '' else ',' end +	ISNULL(LedgerTitle,''),
			@LedgerIdList = @LedgerIdList + Case @LedgerIdList when '' then '' else ',' end +	ISNULL(REPLACE( REPLACE( LedgerId ,']','') ,'[',''),'')
		FROM
		(
			SELECT DISTINCT
				CodeList, LedgerId, LedgerTitle 
			FROM #TempString A
			INNER JOIN VW_ActiveLedgerCodesAndFormula B ON A.CodeList = '['+B.LedgerTotal+']' 
		) K;

		SELECT 
			@ErrorList = @ErrorList + Case @ErrorList when '' then '' else Char(13) end + ISNULL(REPLACE( REPLACE( CodeList ,']','') ,'[',''),'')
		FROM #TempString
		Where SUBSTRING( S, RowNO, RN-RowNO+1 ) NOT IN (SELECT '['+LedgerTotal+']' FROM VW_ActiveLedgerCodesAndFormula);

		SELECT  NULLIF(@ListId,'') as LedgerAbrList, NULLIF(@LedgerCodeList,'') as LedgerCodeList, 
				NULLIF(@LedgerIdList,'') as LedgerIdList, NULLIF(@Formula,'') as Formula,
				NULLIF(@LedgerTitleList,'') as LedgerTitleList, 
				NULLIF(@ErrorList,'') as ErrorList, CASE WHEN NULLIF(@ErrorList,'') IS NULL THEN 0 ELSE 1 END HasError ;
	END;
	
	
	BEGIN TRY DROP TABLE #TempString	END TRY BEGIN CATCH END CATCH;

END;







GO

/****** Object:  StoredProcedure [dbo].[USP_Update_BranchMergeInfo]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_BranchMergeInfo]( @BranchMergeInfoId int, @BranchCodeFrom int,@BranchCodeTo int,@MergeDateM Date,@ReopenDateM Date) AS
BEGIN
	BEGIN TRY
		UPDATE BranchMergeInfo
		SET 
			BranchCodeFrom	= @BranchCodeFrom,
			BranchCodeTo	= @BranchCodeTo, 
			MergeDateM		= @MergeDateM, 
			MergeDateS		= dbo.FN_MiladiToShamsi( @MergeDateM ), 
			ReopenDateM		= @ReopenDateM, 
			ReopenDateS		= dbo.FN_MiladiToShamsi(@ReopenDateM)
		WHERE BranchMergeInfoId= @BranchMergeInfoId;
		RETURN @BranchMergeInfoId;
	END TRY
	BEGIN CATCH 
		RETURN -1;
	END CATCH
	
END;










GO

/****** Object:  StoredProcedure [dbo].[USP_Update_Configs]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_Configs]( @StartFetchDateM Date, @PrefixLetter nvarchar(50),@RemoveComments TinyInt, @RetryMode Tinyint, @AdminCanModify Tinyint) AS
BEGIN
	UPDATE Configs	SET
		StartFetchDateM	= @StartFetchDateM,
		StartFetchDateS = dbo.FN_MiladiToShamsi(@StartFetchDateM),
		PrefixLetter = @PrefixLetter,
		RemoveComments = @RemoveComments,
		RetryMode = @RetryMode,
		AdminCanModify = @AdminCanModify
	WHERE ConfigId = 1
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Update_Formulas]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_Formulas](
					@FormulaId int,
					@SeqNO int, @FormulaType tinyint, @Name nvarchar(200), @Code nvarchar(100), 
					@MethodDesc nvarchar(max), @Context nvarchar(4000), @GeneralId int, 
					@FromLedger nvarchar(4000),@ToLedger nvarchar(4000),@IncludeLedgers nvarchar(4000),					
					@CreateDateM date, @Comment nvarchar(4000), @BranchesIdList nvarchar(max),
					@BranchesNameList nvarchar(max), @BranchTotalList nvarchar(max),
					@WhereClause nvarchar(max), @UsedLedgers nvarchar(max), @UsedLedgersAbr nvarchar(max),
					@UserId int
								) AS
BEGIN
	DECLARE @ErrTitle nvarchar(4000), @CompressedFormulaValue nvarchar(4000), @MyGUID as uniqueidentifier= NEWID();

	DECLARE @Ret int, @OldCode  nvarchar(100);
	-- جهت اطمينان از اينکه شماره توالي و غيره يونيک باشند
	SELECT @Ret= dbo.FN_CheckNewFormula( @FormulaId, @SeqNO, @Name, @Code );
	
	IF @Ret <> 0 
		RETURN @Ret;

	--نگهداري ميشوند log مقادير قبلي جهت 
	SELECT 
		FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, IsPrivate, OwnGUID
	INTO #TEMPLOG
	FROM Formulas
	WHERE FormulaId = @FormulaId

	SELECT @OldCode = Code FROM #TEMPLOG;

	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	BEGIN TRANSACTION 
		BEGIN TRY
			-- Update all log formulas
			IF @OldCode <> @Code
			BEGIN
				UPDATE FormulaRemain	SET Code = @Code	WHERE Code = @OldCode;
				UPDATE ResultFormulas	SET Code = @Code	WHERE Code = @OldCode;

				UPDATE Log_Formulas
					SET UsedLedgersAbr = REPLACE(','+REPLACE( ','+UsedLedgersAbr+',' ,','+@OldCode+',',','+@Code+',')+',',',,','')
				WHERE ','+UsedLedgersAbr+','  like '%,'+@OldCode+',%'

				UPDATE Log_Formulas 	SET  MethodDesc			= REPLACE(MethodDesc,'['+@OldCode+']','['+@Code+']')
				UPDATE Log_Formulas 	SET  Context			= REPLACE(Context,'['+@OldCode+']','['+@Code+']')
				UPDATE Log_Formulas 	SET  CompressedFormula	= REPLACE(CompressedFormula,'['+@OldCode+']','['+@Code+']')
			END
			
			UPDATE	Formulas
			SET
				SeqNO= @SeqNO, 
				FormulaType=@FormulaType, 
				Name=@Name, 
				Code=@Code, 
				MethodDesc=@MethodDesc, 
				Context=@Context, 
				GeneralId=@GeneralId, 
				FromLedger=NULLIF(@FromLedger,''), 
				ToLedger=NULLIF(@ToLedger,''), 
				IncludeLedgers=NULLIF(@IncludeLedgers,''), 
				Comment=@Comment,
				CreateDateM=@CreateDateM, 
				ModifyDateM = GETDATE(), 
				ModifyDateS = dbo.FN_MiladiToShamsi(GETDATE()), 
				BranchesIdList=@BranchesIdList, 
				BranchesNameList=@BranchesNameList, 
				BranchTotalList=@BranchTotalList,
				WhereClause=@WhereClause, 
				UsedLedgers=@UsedLedgers, 
				UsedLedgersAbr=@UsedLedgersAbr,
				UserId = @UserId,
				OwnGUID = @MyGUID
			WHERE 
				FormulaId =@FormulaId
		END TRY
		BEGIN CATCH
			BEGIN TRY DROP TABLE #TEMPLOG END TRY BEGIN CATCH END CATCH
			ROLLBACK TRAN
			RETURN -1;
		END CATCH	

		IF EXISTS( SELECT 1 FROM VW_NextUsedFormula WHERE Code = '['+@Code+']') -- آيا از فرمول جلو رونده استفاده شده است؟
		BEGIN
			BEGIN TRY DROP TABLE #TEMPLOG END TRY BEGIN CATCH END CATCH
			ROLLBACK TRAN;
			RETURN -3;
		END
	-- فرمول مورد نظر به همراه مشتقات آن ساخته ميشود و تست اجرايي ميشود
	-- نتيجه صحيح اجرا يا يک عدد اعشاري است و يا تقسيم بر صفر 
	-- نتيجه فرمول غلط چيزي بجز تفسيم بر صفر خواهد بود

	EXEC USP_Generate_CompressedFormulaForById @FormulaId,1, @ErrTitle OUT, @CompressedFormulaValue OUT;
	SET @ErrTitle ='0.0';
	UPDATE	Formulas SET CompressedFormula= @CompressedFormulaValue WHERE FormulaId =@FormulaId
		
	IF CHARINDEX('Divide by zero error encountered',@ErrTitle,1)=0 -- تقسيم بر صفر رخ نداده است
	BEGIN
		IF TRY_CONVERT(Decimal(18,4),@ErrTitle) IS NULL -- اگر جواب شامل عدد معتبر نبود يعني اينکه فرمول با خطا مواجه شده است
		BEGIN
			BEGIN TRY DROP TABLE #TEMPLOG END TRY BEGIN CATCH END CATCH
			ROLLBACK TRAN;
			RETURN -2;
		END
		ELSE	
			COMMIT TRAN
	END	
	ELSE	
		COMMIT TRAN
	
	EXEC USP_ReCalculate_Formula_ForDrived @Code;
	IF @Code <> @OldCode 
		EXEC USP_ModifyCodeInDriverFormula @OldCode,@Code

	DECLARE @ActTime Datetime= GetDate(), @LogId int, @CheckOld bigint, @CheckNew bigint;
	SELECT
		@CheckOld=
			CHECKSUM( 
				FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context,	GeneralId, FromLedger, ToLedger, IncludeLedgers, 
				CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, 
				WhereClause, UsedLedgers, UsedLedgersAbr, IsPrivate
					)
	FROM #TEMPLOG WHERE FormulaId =@FormulaId;
	
	SELECT
		@CheckNew=
			CHECKSUM( 
				FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context,	GeneralId, FromLedger, ToLedger, IncludeLedgers, 
				CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, 
				WhereClause, UsedLedgers, UsedLedgersAbr, IsPrivate
					)
	FROM Formulas WHERE FormulaId =@FormulaId;

	IF @CheckOld <> @CheckNew
	BEGIN
		INSERT INTO LOG_Formulas
			( ActionDate,LogId,ActType,FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, IsPrivate, OwnGUID)
		SELECT 
				@ActTime,(NEXT VALUE FOR dbo.LogSequence),'UDeleted',FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, IsPrivate, OwnGUID
		FROM #TEMPLOG 

		INSERT INTO LOG_Formulas
			( ActionDate,LogId,ActType,FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, IsPrivate, OwnGUID)
		SELECT 
				@ActTime,(NEXT VALUE FOR dbo.LogSequence),'UInserted',FormulaId, UserId, SeqNO, FormulaType, Name, Code, MethodDesc, Context, GeneralId, FromLedger, ToLedger, IncludeLedgers, CreateDateM, CreateDateS, ModifyDateM, ModifyDateS, Comment, BranchesIdList, BranchesNameList, BranchTotalList, WhereClause, UsedLedgers, UsedLedgersAbr, CompressedFormula, IsPrivate, NEWID()
		FROM Formulas
		WHERE FormulaId =@FormulaId
		
		IF @OldCode <> @Code
		BEGIN
			INSERT INTO LOG_CodeChange( ActionDate , FormulaId , FromCode, ToCode )
			VALUES( @ActTime, @FormulaId, @OldCode, @Code )
		END
	END
	
	BEGIN TRY DROP TABLE #TEMPLOG END TRY BEGIN CATCH END CATCH
END;


GO

/****** Object:  StoredProcedure [dbo].[USP_Update_ImportExcelSchema]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_ImportExcelSchema]( @ImportSchemaId int, @LedgerId int,  @UNC nvarchar(1000),@FolderName nvarchar(1000),@SheetName nvarchar(1000) ) AS
BEGIN
	UPDATE ImportSchemas
	SET
		LedgerId = @LedgerId,
		UNC= @UNC,
		FolderName =@FolderName,
		SheetName =@SheetName
	WHERE ImportSchemaId= @ImportSchemaId
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Update_ImportSchedule_ETL]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_ImportSchedule_ETL] AS
BEGIN
	WITH CTE AS
	(
		SELECT	DISTINCT
			ImportScheduleId,FromDateM,ToDateM,ImportMode,
			1+DATEDIFF(Day,FromDateM,ToDateM)-COUNT(*) OVER ( PARTITION BY ImportScheduleId ) Diff
		FROM ImportSchedule SC
		INNER JOIN TblDate DT ON DT.MiladiDate BETWEEN SC.FromDateM AND SC.ToDateM AND SC.ImportStatus =0
		INNER JOIN CalculationLogs CL ON CL.FetchDateM = DT.MiladiDate AND CL.Status <> 0	
	)

	UPDATE SC
	SET SC.ImportStatus =	 
					CASE IIF(B.ImportScheduleId IS NULL , -1, B.Diff) 
					WHEN 0  THEN  1 -- All Complited
					WHEN -1 THEN -1 -- All Failed
					ELSE 0			-- Some Incomplete
					END 
	FROM ImportSchedule SC
	LEFT JOIN CTE B ON  SC.ImportScheduleId = B.ImportScheduleId

END;


GO

/****** Object:  StoredProcedure [dbo].[USP_Update_NewLedgersStatus]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_NewLedgersStatus]( @LedgerList as nvarchar(max), @Status as tinyint  ) AS
BEGIN

	DECLARE @XML XML= CAST( @LedgerList as xml);
	-- Status 2 -> Accepted so AcceptDate must be updated
	-- Status 3 -> Rejected so RejectDate must be updated
	UPDATE NewLedgers 
	SET 
		AcceptDate = CASE @Status WHEN 2 THEN A.Accept_Date ELSE AcceptDate END,
		RejectDate = CASE @Status WHEN 3 THEN A.Reject_Date ELSE RejectDate END,
		Status =@Status
	FROM
	(
	SELECT 
		Fld.value('NewLedgerId[1]','int')  as New_LedgerId,
		Fld.value('AcceptDate[1]','Date')  as Accept_Date,	 
		Fld.value('RejectDate[1]','Date')  as Reject_Date	 
	FROM @XML.nodes('Record') Tbl(Fld)
	) A
	WHERE NewLedgers.NewLedgerId = A.New_LedgerId

END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Update_UserPassword]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_UserPassword]( @UserId int, @NewPass nvarchar(50) ) AS
BEGIN
	UPDATE Users
		SET
			UserPass= NULLIF(@NewPass,'')
	WHERE UserId = @UserId
END;









GO

/****** Object:  StoredProcedure [dbo].[USP_Update_Users]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[USP_Update_Users]( @UserId int, @UserName nvarchar(50), @UserPass nvarchar(50), @Title nvarchar(100), @AccessMode int, @UserGroupId int ) AS
BEGIN
	UPDATE Users
		SET
			UserName= @UserName,	
			UserPass= NULLIF(@UserPass,''),	
			Title= @Title,	
			AccessMode= @AccessMode,	
			UserGroupId= @UserGroupId	
	WHERE UserId = @UserId
END;

GO

/****** Object:  StoredProcedure [tools].[GetText]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE  PROCEDURE [tools].[GetText]( @RoutineName as nvarchar(max) ) AS    
BEGIN  
	DECLARE @I INT=1, @Len INT, @Line nvarchar(MAX), @S nvarchar(max) ='',@Pos INT, @SeperatorCount INT =0;
	SELECT  @S = definition   
	FROM(
		SELECT definition FROM sys.sql_modules  WHERE object_name(object_id) = @RoutineName 
		UNION
		SELECT base_object_name FROM sys.synonyms WHERE name = @RoutineName
		) A
	SET @Len = Len(ISNULL(@S,''))  
	IF @Len=0	SELECT  @S = @RoutineName, @Len = LEN(@RoutineName)  
  
	WHILE 1=1  
	BEGIN  
		SET @Line ='';  
		SELECT @Pos = Charindex(Char(13)+Char(10), @S), @SeperatorCount =1
		IF @Pos =0 		SELECT @Pos = Charindex(Char(10)+Char(13), @S), @SeperatorCount =1
		IF @Pos =0 		SELECT @Pos = Charindex(Char(10), @S), @SeperatorCount =0
		IF @Pos =0 		SELECT @Pos = Charindex(Char(13), @S), @SeperatorCount =0
		IF @Pos =0 
		BEGIN
			PRINT @S  
			BREAK  
		END  
		SET @Line = LEFT(@S,@Pos-1)  
		PRINT @Line  
		SET @S= STUFF(@S,1,@Pos+@SeperatorCount,'')
	END  
END  
GO

/****** Object:  StoredProcedure [tools].[PrintALL]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

CREATE   PROCEDURE [tools].[PrintALL] ( @List nvarchar(max) ) AS
BEGIN
	Declare @B nvarchar(max) =''
	SELECT 
		@B = Concat(@B, Char(10),'EXEC tools.GetText '+Item, Char(10), 'Print ''GO''') from dbo.FN_StringToTable_Not_NULL(@List, Char(10)) A
	WHERE LTRIM(RTRIM(Item)) not in( Char(13), char(10), Char(32) )
	EXEC(@B)
END
GO

/****** Object:  StoredProcedure [tools].[Printf]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [tools].[Printf]( @TableName nvarchar(4000), @Braket bit =NULL , @Alias varchar(100)= NULL ) AS
BEGIN
	SET NOCOUNT ON  
	DECLARE @ColumnList nvarchar(4000)='', @SchemaName nvarchar(50)='', @PS int ;		
 
	SET @TableName = REPLACE( REPLACE( @TableName ,'[','') ,']','') 
	SET	@PS = CHARINDEX('.',@TableName)
	

	IF @PS = 0	SEt @SchemaName = '' 
	ELSE
	BEGIN
	 	SET @SchemaName= LEFT(@TableName,@PS-1 )
	 	SET @TableName= RIGHT(@TableName,LEN(@TableName)-@PS )
	END
 
	IF (Select COUNT(*) From INFORMATION_SCHEMA.TABLES Where TABLE_NAME =  @TableName)>1 and  @SchemaName=''
			SET  @SchemaName = SCHEMA_NAME(SCHEMA_ID()) 	
	
	
	;With CTE AS
	(
	SELECT  		
		SS.name TABLE_SCHEMA, SO.Name TABLE_NAME , SC.name COLUMN_NAME, Sc.Column_id as ORDINAL_POSITION , TYPE_NAME(Sc.system_type_id) TABLE_TYPE
	FROM Sys.Columns Sc 
	INNER JOIN Sys.objects SO ON SC.object_id = SO.object_id and Type IN('U','V')
	INNER JOIN Sys.schemas SS ON SS.schema_id = SO.schema_id 
	)
 
	SELECT 
		@ColumnList = @ColumnList + 
					CASE ISNULL(@Braket,0) 
						WHEN 0 THEN ','+ COLUMN_NAME ELSE ',['+ COLUMN_NAME + ']'
					END
	FROM CTE C
	WHERE C.TABLE_NAME = @TableName and (@SchemaName = '' OR TABLE_SCHEMA = @SchemaName)
	ORDER BY ORDINAL_POSITION
 
	IF ISNULL(@Alias,'') <>''
		SET @ColumnList=REPLACE(@ColumnList,',',','+@Alias)
	SET @ColumnList=REPLACE(','+@ColumnList,',,','')
	
	PRINT @ColumnList
END;

GO

/****** Object:  StoredProcedure [tools].[RoutineUsed]    Script Date: 7/26/2020 1:33:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

 
-----------------------------------------------

CREATE PROCEDURE [tools].[RoutineUsed]( @RoutineName as varchar(200) ) AS  
BEGIN  
	Declare @Ret Table(Mode tinyint, object_id int,name nvarchar(100), Text nvarchar(4000) )
	
	INSERT INTO @Ret
	SELECT 1 as Mode,object_id,Object_name(object_id) as name, cast( definition as nvarchar(4000)) as Text
	FROM Sys.sql_modules    
	WHERE definition LIKE '%'+@RoutineName+'%'  

	INSERT INTO @Ret
	SELECT 0 as Mode, object_id, name, Type_Desc as Text from SYS.objects  
	WHERE name   LIKE '%'+@RoutineName+'%' and object_id not in( Select object_id from @Ret )

	SELECT NAME,TEXT  from @RET 
	ORDER BY Mode desc

END; 
-----------------------------------------------
GO


