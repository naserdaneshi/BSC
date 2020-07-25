USE [BSC]
GO

/****** Object:  View [dbo].[VW_SeperatedItemFormulas]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_SeperatedItemFormulas] AS
	SELECT  F.FormulaType, '['+F.Code+']' Code, SEQNO, '['+B.Item+']' as UsedCode
	FROM Formulas F (nolock)
	CROSS APPLY dbo.FN_StringToTable_Not_NULL(UsedLedgersAbr,',') B











GO

/****** Object:  View [dbo].[VW_NextUsedFormula]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_NextUsedFormula] AS
	SELECT FormulaType,Code,SeqNO,UsedCode 
	FROM  VW_SeperatedItemFormulas A
	WHERE UsedCode in ( SELECT Code FROM VW_SeperatedItemFormulas WHERE SeqNO > A.SeqNO)











GO

/****** Object:  View [dbo].[VW_GeneralLedgerCoding]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_GeneralLedgerCoding] AS
	SELECT 
		L.LedgerId, L.GeneralId, L.LedgerCode, L.LedgerTitle, L.LedgerAbr, L.LedgerTotal,
		G.GeneralCode, G.GeneralTitle, G.GeneralAbr, 
		'TotalTitle' = G.GeneralTitle+' \ '+L.LedgerTitle,
		'LedgerTitleCode' = Concat(L.LedgerTitle,'(',LedgerCode,')'),
		G.SystemId, S.SystemName 
	FROM Ledgers L
	INNER JOIN Generals G  ON G.GeneralId = L.GeneralId
	INNER JOIN Systems S ON S.SystemId = G.SystemId











GO

/****** Object:  View [dbo].[VW_GeneralActiveLedgers]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_GeneralActiveLedgers] AS
SELECT TOP 100000000
	AL.ActiveLedgerId, AL.LedgerId, AL.Status, LD.SystemId,LD.LedgerAbr,LD.GeneralCode,
	LD.GeneralTitle, LD.GeneralAbr, LD.SystemName,LedgerTitleCode,
	LD.GeneralId,LD.LedgerCode,LD.LedgerTitle,LD.LedgerTotal, LD.TotalTitle,
	'TotalTitleSystem'= CONCAT(SystemName ,'-',LD.TotalTitle) 
FROM ActiveLedgers AL
INNER JOIN VW_GeneralLedgerCoding LD ON AL.LedgerId = LD.LedgerId
ORDER BY LD.GeneralId, LD.LedgerId











GO

/****** Object:  View [dbo].[VW_UNION_LedgersAndFormula]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_UNION_LedgersAndFormula] AS
		SELECT 
			 1 as F_L, FormulaId as Id,Code, Name, Name +' - '+ Code as TotalTitle, '/*'+Name+'*/' as CommTotalTitle
		FROM Formulas
		UNION 
		SELECT	
			 0, LedgerId, LedgerTotal, LedgerTitle, TotalTitle, '/*'+TotalTitle+'*/'
		FROM	VW_GeneralActiveLedgers











GO

/****** Object:  View [dbo].[VW_ResultTrans_FULL]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_ResultTrans_FULL] AS
	SELECT 
		RS.ResultTranId, L.GeneralId, RS.LedgerId, RS.BranchId, G.SystemId, RS.FetchDateM, 
		RS.FetchDateS, RS.Remain, RS.FirstRemain, RS.LastRemain, RS.CreditValue, RS.DebitValue,
		G.GeneralCode, G.GeneralTitle, G.GeneralAbr,
		L.LedgerCode, L.LedgerTitle, L.LedgerAbr, L.LedgerTotal,
		BR.Parent, BR.BranchName,
		S.SystemName 
	FROM dbo.ResultTrans RS
	INNER JOIN dbo.Ledgers  L  ON  L.LedgerId  = RS.LedgerId
	INNER JOIN dbo.Generals G  ON  G.GeneralId = L.GeneralId
	INNER JOIN dbo.Branches BR ON BR.BranchId  = RS.BranchId
	INNER JOIN dbo.Systems  S  ON  S.SystemId  = G.SystemId











GO

/****** Object:  View [dbo].[VW_PivotFormula_Remain]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_PivotFormula_Remain] AS 
SELECT BranchId, FetchDateM,GL_1030_2,GL_1030_3,GL_1030_4,GL_1030_5,GL_1060_1,GL_1120_1,GL_1120_94,GL_1250_10,GL_1250_14,GL_1250_4,GL_1250_8,GL_1250_9 
FROM
(
	SELECT BranchId, FetchDateM,ISNULL(GL_1030_2,0),ISNULL(GL_1030_3,0),ISNULL(GL_1030_4,0),ISNULL(GL_1030_5,0),ISNULL(GL_1060_1,0),ISNULL(GL_1120_1,0),ISNULL(GL_1120_94,0),ISNULL(GL_1250_10,0),ISNULL(GL_1250_14,0),ISNULL(GL_1250_4,0),ISNULL(GL_1250_8,0),ISNULL(GL_1250_9,0)
	FROM ( SELECT BranchId, FetchDateM, LedgerTotal, Remain FROM VW_ResultTrans_FULL ) as TBL 
	PIVOT(
			SUM(Remain) FOR LedgerTotal IN
									(GL_1030_2,GL_1030_3,GL_1030_4,GL_1030_5,GL_1060_1,GL_1120_1,GL_1120_94,GL_1250_10,GL_1250_14,GL_1250_4,GL_1250_8,GL_1250_9)
		) PVT
)
K(BranchId, FetchDateM,GL_1030_2,GL_1030_3,GL_1030_4,GL_1030_5,GL_1060_1,GL_1120_1,GL_1120_94,GL_1250_10,GL_1250_14,GL_1250_4,GL_1250_8,GL_1250_9)











GO

/****** Object:  View [dbo].[VW_ActiveLedgerCodesAndFormula]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_ActiveLedgerCodesAndFormula] AS
	SELECT TOP 10000000
		LedgerId, GeneralId, SystemId, LedgerTotal,LedgerTitle,	TotalTitle
	FROM(
		SELECT	
			LedgerId, GeneralId, SystemId, LedgerTotal,LedgerTitle,	TotalTitle
		FROM	VW_GeneralActiveLedgers
		UNION 

		SELECT FormulaId, -1,-1,Code,Name,Name +' - '+ Code 
		FROM Formulas
		) Tbl
	ORDER BY GeneralId











GO

/****** Object:  View [dbo].[VW_FormulaTree]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_FormulaTree] AS
	WITH CTE_REC AS
	(
		SELECT 0 as Level, CAST(NULL as nvarchar(4000)) as Parent  ,Cast(Code as nvarchar(4000)) Tree,FormulaType,REPLACE(REPLACE(Code,'[',''),']','') Code,SeqNO,UsedCode FROM VW_SeperatedItemFormulas 
		UNION ALL
		SELECT B.Level+1, B.Code,Cast(B.Tree+'\'+A.Code as nvarchar(4000)) , A.FormulaType,REPLACE(REPLACE(A.Code,'[',''),']','') Code,A.SeqNO, A.UsedCode FROM VW_SeperatedItemFormulas A
		INNER JOIN CTE_REC B ON A.Code = B.UsedCode   
	)
	, CTE2 AS
	(
		SELECT 
			DISTINCT
			Level,Parent,A.FormulaType,Tree, A.Code, F.Name, A.SeqNO 
			FROM CTE_REC A
		INNER JOIN Formulas F ON F.SeqNO = A.SeqNO
	)
	, CTE3 AS
	(
		SELECT 
			ROW_NUMBER() OVER ( PARTITION BY Code ORDER BY Level DESC) RW, Level,Parent, FormulaType,Tree, Code,Name, SeqNO 
		FROM CTE2 
	)
	SELECT TOP 10000000
		Level, FormulaType, Tree, Code,Name,Parent
	FROM CTE3
	WHERE RW =1
	ORDER BY Tree 

GO

/****** Object:  View [dbo].[VW_GeneralCoding]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_GeneralCoding] AS
	SELECT TOP 100000000 
		G.GeneralId, G.SystemId, G.GeneralCode, G.GeneralTitle, G.GeneralAbr,
		'GeneralTitleCode'= CONCAT( G.GeneralTitle ,'(', G.GeneralCode,')' ),
		'GeneralTitleCodeSystem'=CONCAT( S.SystemName ,'-',G.GeneralTitle ,'(', G.GeneralCode,')' ),
		S.SystemName 
	FROM Generals G 
	INNER JOIN Systems S ON S.SystemId = G.SystemId
	ORDER BY G.GeneralCode , SystemId











GO

/****** Object:  View [dbo].[VW_TreeGeneralCoding]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_TreeGeneralCoding] AS
	WITH CTE_REC AS
	(
		SELECT 
			-SystemId as Id,
			0 as LevelNO,
			0 Parent,
			0 as Code,
			SystemName as Title,
			'' Abr ,
			SystemName as CodeDesc,
			'' as GeneralTitleCodeSystem,
			SystemName
		FROM Systems
		UNION ALL
		SELECT 
			GeneralId, 
			1 as LevelNO, 
			-SystemId as Parent,
			GeneralCode,
			GeneralTitle,
			GeneralAbr,
			GeneralTitleCode,
			GeneralTitleCodeSystem,
			SystemName
		FROM VW_GeneralCoding
	
	)

	SELECT TOP 10000
		Id, LevelNO, Parent, Code, Title,Abr,
		CodeDesc,GeneralTitleCodeSystem,SystemName
	FROM CTE_REC
	ORDER BY LevelNO,Parent,Id












GO

/****** Object:  View [dbo].[vw_Branch_Remain]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/****** Script for SelectTopNRows command from SSMS  ******/
CREATE VIEW  [dbo].[vw_Branch_Remain]
as
SELECT  b.[BranchId],convert(datetime,FetchDateM) date2,code,sum(remain) as remain
  FROM [ResultFormulas] r
 inner join Branches b
 on r.BranchId=b.BranchId 
 group by b.[BranchId] ,FetchDateM,code











GO

/****** Object:  View [dbo].[VW_BranchesAll]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_BranchesAll] AS
	WITH CTE_REC AS
	(	
		SELECT	 BranchId, Parent, BranchName, CAST(BranchName as nvarchar(4000)) as BranchNameTotal, ';'+CAST(BranchId as nvarchar(4000)) as BranchIdTotal, 
				0 as LevelNO, CAST(N'ãÑ˜Òí' as nvarchar(4000)) NodeDesc	
		FROM	Branches 
		where Parent IS NULL
		UNION ALL
		SELECT	 A.BranchId, A.Parent, A.BranchName, BranchNameTotal+' \ '+A.BranchName	,BranchIdTotal+';'+CAST(A.BranchId as nvarchar(10)),
				B.LevelNO+1, CAST( (CASE B.LevelNO+1	WHEN 0 THEN N'ãÑ˜Òí' WHEN 1 THEN N'ãäÇØÞ' WHEN 2 THEN N'ÓÑ ÑÓÊí' WHEN 3 THEN N'ÔÚÈå' END) As nvarchar(4000))
		FROM	Branches A
		INNER JOIN CTE_REC B ON A.Parent = B.BranchId
		WHERE A.Parent IS NOT NULL

	),
	CTE_RES AS
	(
		SELECT BranchId,Parent,BranchName,BranchNameTotal, BranchIdTotal+';' BranchIdTotal,LevelNO, BranchNameTotal+'( '+NodeDesc+' )' BranchNameTotalDesc
	FROM CTE_REC A	
	)

	Select TOP 10000000000
		BranchId,Parent,BranchName,BranchNameTotal,BranchIdTotal, LevelNO,BranchNameTotalDesc from CTE_RES
	order by LevelNO
GO

/****** Object:  View [dbo].[VW_BranchesAllNoRoot]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_BranchesAllNoRoot] AS
	WITH CTE_REC AS
	(	
		SELECT	 BranchId, Parent, BranchName, 
		CAST(BranchName as nvarchar(4000)) as BranchNameTotal, 
		';'+CAST(BranchId as nvarchar(4000)) as BranchIdTotal, 
		0 as LevelNO,
		CAST(N'ãÑ˜Òí' as nvarchar(4000)) NodeDesc	
		FROM	Branches 
		where Parent IS NULL
		UNION ALL
		SELECT	 A.BranchId, A.Parent, A.BranchName, BranchNameTotal+' \ '+A.BranchName,BranchIdTotal+';'+CAST(A.BranchId as nvarchar(10)),
				B.LevelNO+1, CAST( (CASE B.LevelNO+1	WHEN 0 THEN N'ãÑ˜Òí' WHEN 1 THEN N'ãäÇØÞ' WHEN 2 THEN N'ÓÑ ÑÓÊí' WHEN 3 THEN N'ÔÚÈå' END) As nvarchar(4000))
		FROM	Branches A
		INNER JOIN CTE_REC B ON A.Parent = B.BranchId
		WHERE A.Parent IS NOT NULL

	),
	CTE_RES AS
	(
		SELECT BranchId,Parent,BranchName,BranchNameTotal, BranchIdTotal+';' BranchIdTotal,LevelNO, BranchNameTotal+'( '+NodeDesc+' )' BranchNameTotalDesc
	FROM CTE_REC A	
	)

	SELECT TOP 10000000000
		BranchId,
		Case ISNULL(Parent,66666) WHEN 66666 THEN NULL ELSE Parent END as Parent,
		BranchName,BranchNameTotal, BranchIdTotal,LevelNO-1 as LevelNO ,BranchNameTotalDesc 
	FROM CTE_RES
	WHERE LevelNO >0 
	ORDER BY LevelNO
	
GO

/****** Object:  View [dbo].[VW_DateUsedInLastFetch]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[VW_DateUsedInLastFetch] AS
	SELECT DISTINCT FetchDateM	FROM DayResultTrans
	UNION
	SELECT DISTINCT FetchDateM	FROM OtherBarnchesData












GO

/****** Object:  View [dbo].[VW_Formulas]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Formulas] AS
	SELECT  TOP 1000000000
		FR.FormulaId, FR.SeqNO, FR.FormulaType, FR.Name, FR.Code, FR.MethodDesc, FR.Context, FR.GeneralId, 
		FR.FromLedger, FR.ToLedger, FR.IncludeLedgers, FR.CreateDateM, FR.CreateDateS, 
		FR.ModifyDateM, FR.ModifyDateS, FR.Comment, FR.BranchesIdList, FR.BranchesNameList, FR.BranchTotalList, 
		FR.WhereClause, FR.UsedLedgers, FR.UsedLedgersAbr, UserID, IsPrivate
	FROM  Formulas FR 
	ORDER BY FR.SeqNO











GO

/****** Object:  View [dbo].[VW_FormulasIncludeLOG]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_FormulasIncludeLOG] AS
	SELECT 
		Code 
	FROM LOG_Formulas
	GROUP BY Code
	HAVING Count(*) > 1 












GO

/****** Object:  View [dbo].[VW_Select_Accesses]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_Accesses] AS
	SELECT  AccessId, AccessCode, Title FROM Accesses











GO

/****** Object:  View [dbo].[VW_Select_All_Formula_Elements]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_All_Formula_Elements] AS
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

	SELECT TOP 10000000 
		FormulaId,SeqNo,Code,Code2,Lst 
	FROM CTE_ALL A
	ORDER BY SEQNO











GO

/****** Object:  View [dbo].[VW_Select_Formulas]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_Formulas] AS
	SELECT DISTINCT  TOP 1000000000
		FR.FormulaId, FR.UserId,FR.SeqNO, FR.FormulaType, 
		CASE FR.FormulaType 
			WHEN  0 THEN N'ÔÇãá ãÚíä åÇí'
			WHEN  1 THEN N'ãÚíä ÇÒ ... ÊÇ'
			WHEN  2 THEN N'ãÚíä åÇí ˜á'
			WHEN  3 THEN N'ÏÓÊí'
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
		CASE WHEN ImportStatus IS NULL THEN N'' ELSE N'ÏÇÑÏ' END ImportStatusTitle
	FROM  Formulas FR 
	LEFT JOIN Generals G ON G.GeneralId= FR.GeneralId 
	LEFT JOIN Ledgers LF ON FR.FromLedger = LF.LedgerCode and LF.GeneralId = G.GeneralId
	LEFT JOIN Ledgers LT ON FR.ToLedger = LT.LedgerCode and LT.GeneralId = G.GeneralId
	LEFT JOIN Users    U ON FR.UserId = U.UserId
	LEFT JOIN ImportSchedule SC ON SC.FormulaId = FR.FormulaId
	ORDER BY FR.SeqNO


GO

/****** Object:  View [dbo].[VW_Select_ListOfFormulas]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_ListOfFormulas] AS
	SELECT TOP 1000000
		FormulaId,SeqNO,Name,Code,
		Code+'('+Name +')' as FormulaTitle
	FROM Formulas
	ORDER BY SeqNO











GO

/****** Object:  View [dbo].[VW_Select_ListOfFormulasForCode]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_ListOfFormulasForCode] AS
	SELECT TOP 1000000
		FormulaId,SeqNO,Name,Code,
		Code+'('+Name +')' as FormulaTitle
	FROM Formulas
	ORDER BY Code










GO

/****** Object:  View [dbo].[VW_Select_Similar_Formulas]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_Select_Similar_Formulas]  AS
	WITH CTEPre AS
	( 
		SELECT  SEQNO, B.Item  as GL_Code
		FROM Formulas F 
		CROSS APPLY dbo.FN_StringToTableFormula(Context) B
	),
	CTEResult AS
	(
		SELECT  DISTINCT A.SeqNO,B.Codes FROM CTEPre A
		CROSS APPLY( SELECT GL_CODE+'' FROM CTEPre WHERE SeqNO = A.SeqNO FOR XML PATH('')) B(Codes)
	)

	SELECT 
		'GRP' = DENSE_RANK() OVER( ORDER BY Codes ),SeqNO,Codes 
	FROM CTEResult 
	WHERE Codes IN (SELECT Codes FROM CTEResult GROUP BY Codes HAVING COUNT(*)>1 )











GO

/****** Object:  View [dbo].[VW_TreeLedgerCoding]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_TreeLedgerCoding] AS
SELECT 
	-GeneralId as Id, 
	0 as Parent, 
	0 as LevelNO,
	GeneralCode as Code, 
	GeneralTitle as Title, 
	GeneralAbr as Abr, 
	GeneralTitle+' ('+GeneralAbr+')' as CodeDesc 
FROM Generals
UNION ALL
SELECT 
	LedgerId,
	-GeneralId as Parent,
	1 as LevelNO,
	LedgerCode as Code,
	LedgerTitle as Title,
	LedgerTotal as Abr,
	LedgerTitle+' ('+LedgerTotal+')' as CodeDesc
FROM Ledgers












GO

/****** Object:  View [dbo].[VW_UserGroups]    Script Date: 7/26/2020 1:30:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VW_UserGroups] AS
	Select UserGroupId, GroupName  from UserGroups











GO


