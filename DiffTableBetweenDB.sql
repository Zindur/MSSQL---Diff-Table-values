
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DiffTableBetweenDB]') AND type in (N'P', N'PC'))
	DROP PROCEDURE [dbo].[DiffTableBetweenDB]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Oleg Ciobanu 
-- Create date: 20170915
-- compare Data from same table between databases Current (Actual) and another (BaseLine)
-- results will be stored into table dbo.TestCHK_SP_ + schema name + table name
-- by default will display just top @Top records from both combinations
-- removed restriction to validate other schemas
-- =============================================
CREATE PROCEDURE [dbo].[DiffTableBetweenDB] 
   @ComparisonDatabaseName NVARCHAR (250) -- pass DB from another DB
  ,@TableToCompare NVARCHAR (250) -- Table to compare (SchemaName.TableName)
  ,@Top INT = 10 -- return just first 10 records by default
  ,@OrderByColumn VARCHAR(250) = NULL -- ordered column
  ,@ExcludeColumns VARCHAR(1000) = NULL -- comma delimited list of columns what will be excluded from diff
  ,@JoinFKTables INT = 1 -- generate joins by FK keys - add (join) tables
  ,@UseCollation INT = 0 -- this param was added just for reducing the size of building queries, because SQL is limitetd to 8000 of chars
						 -- and you can have this issue usually when @JoinFKTables = 1 and is joining lots of tables with lots of columns
  ,@ExcludeNonKeyFKColumns INT = 1 -- include just Key columns on Joins if exists 
  ,@DetailedOutput INT = 1 -- suppress detailed input of comparation or not
  ,@DetailedPrint INT = 1 -- suppress print messages with details or not
  ,@WhereClause NVARCHAR (1000) = '' -- is good to add alias "mt." on each column, specially when exist similar column on JOIN table (when @JoinFKTables = 1)
AS
BEGIN	
	SET NOCOUNT ON;	
	
	-- Declare variables Section
	DECLARE @DBActual NVARCHAR(250)   = QUOTENAME(( SELECT DB_NAME() )) -- Current DB
	DECLARE @DBBaseLine NVARCHAR(250) = QUOTENAME(REPLACE(REPLACE(@ComparisonDatabaseName,']',''),'[',''))
	
	IF @DetailedPrint = 1 BEGIN
		PRINT 'DBActual:' + COALESCE(@DBActual,'')
		PRINT 'DBBaseLine:' + COALESCE(@DBBaseLine,'')
	END
	 
	DECLARE @ExcludeCrosses INT = 1	
	DECLARE @ExcludeFKCrosses INT = 1
	DECLARE @IsDebuging INT = 0
	DECLARE @TableList nvarchar(max) = @TableToCompare
 
	---***************** USE DB Name
	IF (QUOTENAME((SELECT DB_NAME())) = IsNUll(@DBBaseLine,''))  BEGIN
		DECLARE @Msg nvarchar(250);
		SET @Msg = 'Wrong DB connection: ' + (SELECT DB_NAME()) + ', Connection must be diff from : ' + IsNUll(@DBBaseLine,'Null')
		SELECT @Msg
		RAISERROR (@Msg ,1,1)
		RETURN
	END;  
	---*************************************************************
	IF @IsDebuging = 1
		SELECT @ExcludeCrosses AS '[@ExcludeCrosses - to exclude same column name]'
			  ,@ExcludeFKCrosses AS '[@ExcludeFKCrosses - to exclude FK referenced Identity columns]'
 
	DECLARE @Columns  nvarchar (max)		-- build Columns
	DECLARE @ColumnsExcludedDeletedDateColumns  nvarchar (max)		-- build Columns
	DECLARE @JoinColumnsExcludedDeletedDateColumns  nvarchar (max)		-- build Columns
	DECLARE @ColumnsOrder nvarchar (max)	-- Order of Columns like Order By 1,2,3
	DECLARE @ColumnsCHK  nvarchar (max)	 -- build Columns with CHK per column   
	DECLARE @ColumnsChanged nvarchar (max)  -- build List of Columns what have Diffs
	DECLARE @JoinColumns  nvarchar (max)	-- build JoinColumns	
	DECLARE @JoinTables nvarchar (max)	  -- build JOIN tables	
	DECLARE @Qry nvarchar (max)			 -- Query with build Columns
	DECLARE @QryInfo nvarchar (max) 
	DECLARE @SchemaName	nvarchar (128)	
	DECLARE @TableName	 nvarchar (128)
	DECLARE @IgnoreTables  table (IgnoreShemaName NVARCHAR(128), IgnoreTableName NVARCHAR(128)); -- List of Tables what will be Ignored and excluded	
	DECLARE @IgnoreColumns table (IgnoreColumnName varchar (128));							   -- List of Columns what will be Ignored and excluded from @Columns
	DECLARE @RecCount bigint;			   -- Count of Recs from tables
	DECLARE @ExceptAvsBL bigint;			-- for calc number of diffs AB
	DECLARE @ExceptBLvsA bigint;			-- for calc number of diffs BA
	DECLARE @OutputIntoTableName nvarchar (256);
	DECLARE @OutputIntoTableSelect nvarchar (max);
	--
	DECLARE @ZeroNullVal NVARCHAR(2) = '1';
	DECLARE @ExecDate DateTime = convert(datetime, convert(char(20), GetDate(), 20));

	-- Counter
	DECLARE @i  int = 0  -- loop counter
	DECLARE @ii int = @i -- Max value of counter

	-- Insert Ignored Tables
	INSERT INTO @IgnoreTables (IgnoreShemaName,IgnoreTableName)
	VALUES ('IgnoreSchemaname','IgnoreTableName') -- TODO 01 make Generic or configurable

	-- Insert Ignored Columns
	--INSERT INTO @IgnoreColumns (IgnoreColumnName)
	--VALUES ('DeletedDate')
	;

	-- Build list of looking Tables
	IF OBJECT_ID('tempdb..#TableToCompare') IS NOT NULL DROP TABLE #TableToCompare
	
	CREATE TABLE #TableToCompare(
		[stResult] [nvarchar](max) COLLATE DATABASE_DEFAULT NULL,
		[SchemaName] [nvarchar](128) COLLATE DATABASE_DEFAULT NULL,
		[TableName] [nvarchar](128) COLLATE DATABASE_DEFAULT NULL
	) ON [PRIMARY]	
	
	--*
	-- Populate #TableToCompare from @TableList 
	--*
	DECLARE @x XML 
	SELECT @x = CAST('<A>' + REPLACE(-- remove all dogy chars
										REPLACE(REPLACE(@TableList,CHAR(10),''),CHAR(9),'')
										,CHAR(13),'</A><A>')
							+ '</A>' AS XML)
	;WITH cte as (
		SELECT RTRIM(LTRIM(t.value('.', 'nvarchar(250)'))) AS stResult
		FROM @x.nodes('/A') AS x(t) 
		)
	INSERT INTO #TableToCompare
	SELECT stResult
			,LTRIM(RTRIM(PARSENAME (REPLACE( stResult, '_', '.' ), 2 ))) AS SchemaName
			,LTRIM(RTRIM(PARSENAME (REPLACE( stResult, '_', '.' ), 1 ))) AS TableName
	FROM cte;

	
	-- clean up
	DELETE FROM #TableToCompare
	WHERE stResult = '' or SchemaName is Null
	 
	--select * from #TableToCompare	
	
	
	--
	-- Build List with All Schemas,Tables,Columns, Is Identity Column
	-- or Is UpdatedDateTime - columns what gets GetDate()
	
	IF OBJECT_ID('tempdb..#TableList') IS NOT NULL DROP TABLE #TableList
	IF OBJECT_ID('tempdb..#ResultTable') IS NOT NULL DROP TABLE #ResultTable	
	IF OBJECT_ID('tempdb..#SchemaDiffTable') IS NOT NULL DROP TABLE #ResultTable
	CREATE TABLE #SchemaDiffTable (
								 DiffType varchar(20) COLLATE DATABASE_DEFAULT
								,ColumnName varchar(128) COLLATE DATABASE_DEFAULT
								)

	IF OBJECT_ID('tempdb..#GetDateColumnTable') IS NOT NULL DROP TABLE #GetDateColumnTable

	CREATE TABLE #GetDateColumnTable ( 
								GetDateColumnTableId int identity not null
							   ,SchemaName varchar (128) COLLATE DATABASE_DEFAULT
							   ,TableName  varchar (128) COLLATE DATABASE_DEFAULT
							   ,ColumnName varchar (128) COLLATE DATABASE_DEFAULT
							  );
	-- Populate working table #GetDateColumnTable
	-- with data where Columns what is "GetDate()" or GetUTCDate() or other ... are updating on everry ETL run
	-- thos columns will be replaced with constant
	-- can be used wild char "*" for SchemaName and TableName columns
	-- TODO 02 make Generic or configurable
	INSERT INTO #GetDateColumnTable (SchemaName, TableName, ColumnName) 
	SELECT '*','*','DeletedDate'
	UNION ALL
	SELECT '*','*','UpdatedDateTime'
	UNION ALL
	SELECT 'SchemaName','TableName','ColumnName'	
	
	CREATE TABLE #ResultTable ( SchemaName varchar (128) COLLATE DATABASE_DEFAULT
							   ,TableName  varchar (128) COLLATE DATABASE_DEFAULT
							   ,CHK		bigint
							   ,RecCount   bigint
							  );
	
	SELECT
	   rn_ord = ROW_NUMBER() OVER (PARTITION BY (select Null) ORDER BY  lt.rn, lt.SchemaName, lt.TableName, ic.COLUMN_NAME)
	  ,lt.rn as GroupTable
	  ,lt.SchemaName, lt.TableName
	  ,ic.COLUMN_NAME as ColumnName
	  ,COLUMNPROPERTY(object_id(ic.table_schema + N'.' + ic.TABLE_NAME), ic.COLUMN_NAME, 'IsIdentity') AS IsIdentity
	  ,0 as IsFKtoRefIdentity -- is foreign key to Identity column
	  ,0 as IsUpdatedDateTime
	  ,N'' AS Qry
	  ,ic.DATA_TYPE
	INTO #TableList 
	FROM ( SELECT
				rn = ROW_NUMBER() OVER (PARTITION BY (select Null) ORDER BY  sc.name, ta.name)   
			  , sc.name AS SchemaName
			  , ta.name AS TableName
			FROM sys.tables ta
			  --INNER JOIN sys.partitions pa ON pa.OBJECT_ID = ta.OBJECT_ID
			  INNER JOIN sys.schemas sc ON ta.schema_id = sc.schema_id
									   -- Ignore Schemas
									   AND sc.name NOT IN ('' 
														--,'dbo'
														  )
			WHERE 
			   -- exclude tables
			   NOT (sc.name = 'ExcludeSchemaName' AND ta.name = 'ExcludeTableName') -- TODO 01 make Generic or configurable
		  ) lt	
	  INNER JOIN information_schema.COLUMNS ic ON ic.table_name = lt.TableName
											   and ic.table_schema = lt.SchemaName
	  -- filter and select just tables passed by TableList, this line is individual per current script, dint merge with other scripts if is not necessary
	  LEFT JOIN #TableToCompare clt ON clt.SchemaName = lt.SchemaName
								   AND clt.TableName = lt.TableName
	  LEFT JOIN @IgnoreTables it ON it.IgnoreShemaName = lt.SchemaName
								AND it.IgnoreTableName = lt.TableName
	WHERE 
			ic.DATA_TYPE NOT IN (N'image', N'xml', N'text')	   
		AND it.IgnoreTableName IS NULL
		-- conditional join
		-- if @JoinFKTables = 1 then select all data
		AND ( @JoinFKTables = 1
				OR (@JoinFKTables <> 1 AND clt.TableName IS NOT NULL)
			)
	ORDER BY   
	  lt.SchemaName, lt.TableName
	--Select * from #TableList
	
	--Identify if column is part of FK and reference is Identity column
	;WITH cte_FK as (
		SELECT f.name AS ForeignKey
		   ,RTRIM(schema_name(ObjectProperty(f.parent_object_id,'schemaid'))) as SchemaName
		   ,OBJECT_NAME(f.parent_object_id) AS TableName
		   ,COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName
		   ,RTRIM(schema_name(ObjectProperty(f.referenced_object_id,'schemaid'))) as ReferenceSchemaName
		   ,OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName
		   ,COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName 
		   ,COLUMNPROPERTY(f.referenced_object_id, COL_NAME(fc.referenced_object_id, fc.referenced_column_id), 'IsIdentity') AS ReferenceIsIdentity
		FROM sys.foreign_keys AS f 
		INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
	)
	--select tl.* , fkid.*
	UPDATE tl
		SET IsFKtoRefIdentity = 1
	FROM #TableList tl
	INNER JOIN (
			  SELECT nonId.rn_ord
					--,nonId.ReferenceSchemaName,nonId.ReferenceTableName,nonId.ReferenceColumnName
					--,nonId.ReferenceIsIdentity
			  FROM (
				  -- select non identity columns but part of FK
				  SELECT tl2.rn_ord
						--,fk.ReferenceSchemaName,fk.ReferenceTableName,fk.ReferenceColumnName
						--,fk.ReferenceIsIdentity
				  FROM #TableList tl2
				  INNER JOIN cte_FK fk ON fk.SchemaName = tl2.SchemaName
									  AND fk.TableName = tl2.TableName
									  AND fk.ColumnName = tl2.ColumnName
				  WHERE tl2.IsIdentity = 0
					AND fk.ReferenceIsIdentity in (1) -- filter: select just if Reference Column is Identity
				  ) nonId
	 ) fkId ON fkid.rn_ord = tl.rn_ord
	-- Select * from #TableList			
	 
	-- Get right(end) margin -- where loop will stop  
	SET @ii = (SELECT MAX(GroupTable) FROM #TableList)  
	
	-- Set IsUpdatedDateTime columns
	-- Update All where Column Name = UpdatedDateTime  
	UPDATE #TableList
	SET IsUpdatedDateTime = 1
	WHERE UPPER(ColumnName) = 'UPDATEDDATETIME'  
	
	-- Update Columns what is "GetDate()" or GetUTCDate() or other ... just exclude ;)
	UPDATE #TableList
	SET IsUpdatedDateTime = 1
	WHERE UPPER(SchemaName) = UPPER('SchemaName')
	  AND ( 
			(	UPPER(ColumnName) = UPPER('ColumnName') -- TODO 03 make Generic or configurable
			 AND UPPER(TableName)  = UPPER('TableName'))
			 OR
			(	UPPER(ColumnName) = UPPER('ColumnName2')
			 AND UPPER(TableName)  = UPPER('TableName2'))
	;  


	IF (@JoinFKTables = 1) BEGIN
		-- FK build
		-- #TableListFK is same like #TableList but with additional columns and as result extra recs, use Distinct to remove dups for old way to retrive data
		-- todo: replace #TableList with #TableListFK and leave just one table
        ;WITH cteGetKeyColumns AS (
						-- Build "Key" Columns into fk_IsKeyColumn
						-- see param: @ExcludeNonKeyFKColumns
						SELECT 
							s.name as SchemaName
							,t.name As TableName
							,c.name  As ColumnName
							-- ordering columns
							,ROW_NUMBER() OVER (PARTITION BY s.name, t.name 
												ORDER BY 
													CASE 
														WHEN c.name like '%Key' THEN '1' -- TODO 06 make Generic or configurable
														WHEN c.name like '%Code' THEN '2'
														WHEN c.name like '%Name' THEN '3'
														ELSE c.name
													END
												) AS rn
						FROM sys.columns c
						INNER JOIN sys.objects t ON t.object_id = c.object_id
						INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
						-- note !!!
						-- check if column is part of unique index
						-- this should reduce the list
						-- just comment next tables if you would like columns what are not part of
						-- unique index
						INNER JOIN sys.indexes ind ON ind.object_id = t.object_id
													AND ind.is_unique = 1
						INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id
							AND ind.index_id = ic.index_id
							AND ic.column_id = c.column_id
							AND ic.is_included_column = 0
						WHERE 
							(c.name like '%Key' OR c.name like '%Code' OR c.name like '%Name') -- TODO 06 make Generic or configurable
							AND s.name in ('RequiredSchemaName') -- TODO 04 make Generic or configurable
						--ORDER BY 1,2
					)
		,cte_FK as (
            SELECT f.name AS ForeignKey
               ,RTRIM(schema_name(ObjectProperty(f.parent_object_id,'schemaid'))) as SchemaName
               ,OBJECT_NAME(f.parent_object_id) AS TableName
               ,COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName
               ,RTRIM(schema_name(ObjectProperty(f.referenced_object_id,'schemaid'))) as ReferenceSchemaName
               ,OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName
               ,COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName 
               ,COLUMNPROPERTY(f.referenced_object_id, COL_NAME(fc.referenced_object_id, fc.referenced_column_id), 'IsIdentity') AS ReferenceIsIdentity
            FROM sys.foreign_keys AS f 
            INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
        )   
        SELECT lt.*--, fkId.* 
        , lt2.rn_ord as fk_rn_ord
        , lt2.GroupTable  as fk_GroupTable
        , lt2.SchemaName as fk_SchemaName
        , lt2.TableName as fk_TableName
        , lt2.ColumnName as fk_ColumnName
        , lt2.IsIdentity as fk_IsIdentity
        , lt2.IsFKtoRefIdentity as fk_IsFKtoRefIdentity
        , lt2.IsUpdatedDateTime as fk_IsUpdatedDateTime
        , lt2.DATA_TYPE as fk_DATA_TYPE
		, COALESCE(kc.rn,0) AS fk_IsKeyColumn
        INTO #TableListFK
        FROM #TableList lt
            LEFT JOIN ( -- follow code (JOIN) copied from FK build where IsFKtoRefIdentity is set to 1
                      SELECT nonId.rn_ord
                            ,nonId.ReferenceSchemaName,nonId.ReferenceTableName,nonId.ReferenceColumnName
                            --,nonId.ReferenceIsIdentity
                      FROM (
                          -- select non identity columns but part of FK
                          SELECT tl2.rn_ord
                                ,fk.ReferenceSchemaName,fk.ReferenceTableName,fk.ReferenceColumnName
                                --,fk.ReferenceIsIdentity
                          FROM #TableList tl2
                          INNER JOIN cte_FK fk ON fk.SchemaName = tl2.SchemaName
                                              AND fk.TableName = tl2.TableName
                                              AND fk.ColumnName = tl2.ColumnName
                          WHERE tl2.IsIdentity = 0
                            AND fk.ReferenceIsIdentity IN (1) -- filter: select just if Reference Column is Identity
                          ) nonId
             ) fkId ON fkid.rn_ord = lt.rn_ord
        LEFT JOIN #TableList lt2 ON lt2.SchemaName = fkId.ReferenceSchemaName
                                AND lt2.TableName = fkId.ReferenceTableName
		-- filter just to table to compare
		INNER JOIN #TableToCompare clt ON clt.SchemaName = lt.SchemaName
								   AND clt.TableName = lt.TableName
		LEFT JOIN cteGetKeyColumns kc ON kc.SchemaName = lt2.SchemaName
									AND kc.TableName = lt2.TableName
									AND kc.ColumnName = lt2.ColumnName
									AND kc.rn = 1
        ORDER BY   
          lt.SchemaName, lt.TableName 
	END;
	
	-- clean up
	DELETE lt
	FROM #TableList lt
	LEFT JOIN #TableToCompare clt ON clt.SchemaName = lt.SchemaName
							   AND clt.TableName = lt.TableName
	WHERE clt.TableName IS NULL
	
	IF @IsDebuging = 1  BEGIN
		select '#TableList' as TableName, * from #TableList
		select '@IgnoreColumns' as TableName, * from @IgnoreColumns;
		IF OBJECT_ID('tempdb..#TableListFK') IS NOT NULL
			select '#TableListFK' as TableName, * from #TableListFK
	END
	
	-- drop all TestCHK tables
	
	SET @Qry = NULL			  
	SELECT @Qry = COALESCE(@Qry, '') + 'DROP TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']'  + CHAR(13) 
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = 'dbo'
		AND TABLE_NAME LIKE 'TestCHK_SP_%';
	IF @DetailedPrint = 1 BEGIN
		PRINT ''	
		PRINT @Qry  
	END
	IF NULLIF(@Qry,'') IS NOT NULL BEGIN
		EXEC (@Qry)
	END;

	
	-- Start Looping/Processing
	WHILE @i < @ii BEGIN
		SET @i = @i + 1;
		-- PRINT '@i := ' + CAST(@i AS VARCHAR(9))
		-- Processing 
		SET @TableName  = (SELECT TOP 1 TableName  FROM #TableList WHERE GroupTable = @i)
		SET @SchemaName = (SELECT TOP 1 SchemaName FROM #TableList WHERE GroupTable = @i)
		-- init variables
		SET @Columns = Null
		SET @ColumnsExcludedDeletedDateColumns = Null
		SET @JoinColumnsExcludedDeletedDateColumns = Null
		SET @ColumnsOrder = NUll
		SET @JoinColumns = '';
		SET @JoinTables = '';		
		
		IF OBJECT_ID('tempdb..#ColumnsList') IS NOT NULL DROP TABLE #ColumnsList
		

		IF (@SchemaName + @TableName) IS NOT NULL BEGIN 
			-- Build Column list
			;WITH steColumns as
			(
				SELECT 
					   1 as rn
					  ,t.ColumnName as ColumnName
					  ,CASE WHEN gdt.ColumnName IS NULL 
							THEN 0
							ELSE 1 
					   END as IsGetDateColumnName
					  ,t.DATA_TYPE
				FROM 
					#TableList t
				LEFT JOIN #GetDateColumnTable gdt ON REPLACE(gdt.SchemaName, '*', @SchemaName) = @SchemaName
								AND REPLACE(gdt.TableName, '*', @TableName) = @TableName
								AND gdt.ColumnName = t.ColumnName  
				WHERE 
					  t.GroupTable = @i
				  --* Excluding Columns from List
				  --  Excluding Identity
				  AND t.IsIdentity = 0
				  --  Excluding IsUpdatedDateTime
				  AND t.IsUpdatedDateTime = 0 
				  AND t.ColumnName NOT IN ( SELECT 
												IgnoreColumnName 
											FROM 
												@IgnoreColumns
											)
				  -- exclude if column is FK and is reference column is Identity
				  AND IsNull(t.IsFKtoRefIdentity,0) IN (0, CASE WHEN @ExcludeFKCrosses = 1 THEN 0 ELSE 1 END)
				  -- Exclude All columns where:
				  -- Exclude and if Column exist in other tables
				  AND t.ColumnName NOT IN (SELECT
											 ColumnName
										   FROM   
											 #TableList
										   WHERE 
												 GroupTable <> @i
											 AND IsIdentity = 1  
											 AND 1 = @ExcludeCrosses
										   )
				  AND t.ColumnName NOT IN (SELECT
											 ColumnName
										   FROM   
											 #TableList
										   WHERE 
												  GroupTable <> @i
											 AND IsUpdatedDateTime = 1 
											 AND 1 = @ExcludeCrosses 
										   )
				  AND t.ColumnName NOT IN (	-- ignore custom/passed columns
											   SELECT t.ColumnName as IgnoreColumnName
											FROM (
												SELECT LTRIM(Split.a.value('.', 'VARCHAR(max)')) AS ColumnName
												FROM
												( SELECT CAST ('<M>' + REPLACE(@ExcludeColumns, ',', '</M><M>') + '</M>' AS XML) AS Data 
												) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
											) t
											WHERE t.ColumnName <> ''
										)
			)
			SELECT rn, ColumnName ,IsGetDateColumnName, DATA_TYPE
				  ,CAST('' AS VARCHAR(128)) AS fk_ColumnName
				  ,0 as rn_ord
				  ,0 as rn_join
				  , 'mt' as TypeTable -- mt = master table
			INTO #ColumnsList
			FROM steColumns
			
			-- scheck if is schema is the same
			-- number of columns must be pair if is the same
			-- note: we should eclude columns what are excluded from validation
			SET @Qry = 'INSERT INTO #SchemaDiffTable
						SELECT  k.DiffType, k.COLUMN_NAME
						FROM 
						(
						SELECT 
							''In BL - Not In Act'' COLLATE DATABASE_DEFAULT DiffType
							,ic.COLUMN_NAME COLLATE DATABASE_DEFAULT COLUMN_NAME
						FROM ' + @DBBaseLine + '.sys.tables ta
						INNER JOIN ' + @DBBaseLine + '.sys.schemas sc ON ta.schema_id = sc.schema_id
						INNER JOIN ' + @DBBaseLine + '.information_schema.COLUMNS ic ON ic.table_name = ta.Name
														   and ic.table_schema = sc.Name
						--INNER JOIN #ColumnsList cl ON cl.ColumnName = ic.COLUMN_NAME
						WHERE sc.name = ''' + @SchemaName + '''
						  AND  ta.name = ''' + @TableName + '''
						EXCEPT
						SELECT ''In BL - Not In Act''
								, ic2.COLUMN_NAME 
						FROM ' + @DBActual + '.sys.tables ta2
						INNER JOIN ' + @DBActual + '.sys.schemas sc2 ON ta2.schema_id = sc2.schema_id
						INNER JOIN ' + @DBActual + '.information_schema.COLUMNS ic2 ON ic2.table_name = ta2.Name
														   and ic2.table_schema = sc2.Name
						--INNER JOIN #ColumnsList cl2 ON cl2.ColumnName = ic2.COLUMN_NAME
						WHERE sc2.name = ''' + @SchemaName + '''
						  AND  ta2.name = ''' + @TableName + '''
						UNION ALL
						SELECT 
							''In Act - Not In BL'' COLLATE DATABASE_DEFAULT DiffType
							,ic.COLUMN_NAME COLLATE DATABASE_DEFAULT COLUMN_NAME
						FROM ' + @DBActual + '.sys.tables ta
						INNER JOIN ' + @DBActual + '.sys.schemas sc ON ta.schema_id = sc.schema_id
						INNER JOIN ' + @DBActual + '.information_schema.COLUMNS ic ON ic.table_name = ta.Name
														   and ic.table_schema = sc.Name
						--INNER JOIN #ColumnsList cl ON cl.ColumnName = ic.COLUMN_NAME
						WHERE sc.name = ''' + @SchemaName + '''
						  AND  ta.name = ''' + @TableName + '''
						EXCEPT
						SELECT ''In Act - Not In BL''
								,ic2.COLUMN_NAME 
						FROM ' + @DBBaseLine + '.sys.tables ta2
						INNER JOIN ' + @DBBaseLine + '.sys.schemas sc2 ON ta2.schema_id = sc2.schema_id
						INNER JOIN ' + @DBBaseLine + '.information_schema.COLUMNS ic2 ON ic2.table_name = ta2.Name
														   and ic2.table_schema = sc2.Name
						--INNER JOIN #ColumnsList cl2 ON cl2.ColumnName = ic2.COLUMN_NAME
						WHERE sc2.name = ''' + @SchemaName + '''
						  AND  ta2.name = ''' + @TableName + '''
						  ) k'
			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT 'Validate schema:'
				PRINT @Qry
				PRINT '*************'
			END
			EXEC (@Qry)

			--EXEC sp_executesql @Qry, N'@rowcount int output', @rowcount output;

			
			IF EXISTS (SELECT TOP 1 * FROM #SchemaDiffTable) BEGIN
				PRINT ''
				PRINT 'Schema are not the same !!! Exiting from validation'
				PRINT ''
				SELECT 'Schema are not the same !!! Exiting from validation' as 'Warning'

				SELECT DiffType
					, @SchemaName + '.'+ @TableName As TableName
					, ColumnName AS ColumnsMismatchBySchema
				FROM #SchemaDiffTable

				TRUNCATE TABLE #ColumnsList
				SET @JoinFKTables = 0
				-- and exit 
			END
			-- end schema validation
			
			
			IF (@JoinFKTables = 1) BEGIN
				-- nearly same like cte steColumns but with additional filters and columns
				-- process JOINS  
				;WITH steJoinColumns as
				(
					SELECT DISTINCT
						   t.rn_ord
						  ,t.ColumnName as ColumnName
						  ,1 as rn
						  ,t.fk_ColumnName as fk_ColumnName
						  --,CASE WHEN gdt.ColumnName IS NULL 
						  --	THEN 0
						  --	ELSE 1 
						  -- END as IsGetDateColumnName
						  ,CASE WHEN gdtfk.ColumnName IS NULL 
								THEN 0
								ELSE 1 
						   END as fk_IsGetDateColumnName
						  ,t.fk_DATA_TYPE
						  -- use rn_join instead of rn_ord for building alias name for join tables
						  -- by this should be less modified files generated
						  ,DENSE_RANK() OVER (ORDER BY t.rn_ord) AS rn_join
					FROM 
					  #TableListFK t
					--LEFT JOIN #GetDateColumnTable gdt ON REPLACE(gdt.SchemaName,'*',t.SchemaName) = t.SchemaName
					--			AND REPLACE(gdt.TableName,'*',t.TableName) = t.TableName
					--			AND gdt.ColumnName = t.ColumnName
					LEFT JOIN #GetDateColumnTable gdtfk ON REPLACE(gdtfk.SchemaName,'*',t.fk_SchemaName) = t.fk_SchemaName
								AND REPLACE(gdtfk.TableName,'*',t.fk_TableName) = t.fk_TableName
								AND gdtfk.ColumnName = t.fk_ColumnName
					WHERE 
						  t.GroupTable = @i
					  --* Excluding Columns from List
					  --  Excluding Identity
					  AND t.IsIdentity = 0
					  --  Excluding IsUpdatedDateTime
					  AND (t.IsUpdatedDateTime = 0 AND t.fk_IsUpdatedDateTime = 0)
					  AND t.ColumnName NOT IN (SELECT 
												ic.IgnoreColumnName 
											  FROM 
												@IgnoreColumns ic)
					  -- exclude if column is FK and is reference column is Identity
					  AND IsNull(t.IsFKtoRefIdentity, 0) IN (0, CASE WHEN @ExcludeFKCrosses = 1 AND @JoinFKTables = 0 THEN 0 ELSE 1 END) --***--
					  -- Exclude All columns where:
					  -- Exclude and if Column exist in other tables
					  AND t.fk_ColumnName NOT IN (SELECT DISTINCT
												 t1.ColumnName
											   FROM   
												 #TableListFK t1
											   WHERE 
													 t1.GroupTable <> @i
												 AND t1.IsIdentity = 1  
												 AND 1 = 0 --@ExcludeCrosses
											   )
					  AND t.fk_ColumnName NOT IN (SELECT DISTINCT
												 t2.ColumnName
											   FROM   
												 #TableListFK t2
											   WHERE 
													  t2.GroupTable <> @i
												 AND (t2.IsUpdatedDateTime = 1 OR t2.fk_IsUpdatedDateTime = 1)

												 AND 1 = 0 --@ExcludeCrosses 
											   )
				-- @ExcludeNonKeyFKColumns
				-- join just "Key" columns if @ExcludeNonKeyFKColumns is Enabled see cte: cteGetKeyColumns
				-- if "Key" columns are missing then include all of them
                      AND t.fk_ColumnName IN (	-- Select just Key Columns and Identity
												SELECT DISTINCT 
													--*, 
													--t3.fk_rn_ord
													t3.fk_ColumnName
												FROM   
													#TableListFK t3
												WHERE 
													t3.fk_rn_ord IS NOT NULL
												AND (1=1
														AND t3.fk_IsIdentity = 1
														AND t3.GroupTable = @i
													)

												OR  (
														t3.GroupTable = @i
														AND
														t3.fk_rn_ord = (CASE 
																		WHEN 0 = @ExcludeNonKeyFKColumns
																		THEN t3.fk_rn_ord
		  
																		WHEN EXISTS (select 1 from #TableListFK t4 where t4.fk_GroupTable = t3.fk_GroupTable
																														and t3.GroupTable = @i
																														and t3.fk_IsKeyColumn = 1)
																		THEN t3.fk_rn_ord

										
																		WHEN NOT EXISTS (select 1 from #TableListFK t5 where t5.fk_GroupTable = t3.fk_GroupTable
																														and t5.GroupTable = @i
																														and t5.fk_IsKeyColumn = 1)
																		THEN t3.fk_rn_ord
										
										
																		ELSE -1
																	END
																	)

													)
                                               ) 											   
					  AND t.fk_IsFKtoRefIdentity = 0
					  AND t.fk_IsIdentity = 0
				)
				INSERT INTO #ColumnsList
				--SELECT rn_ord, fk_ColumnName ,fk_IsGetDateColumnName, DATA_TYPE, 'st' as TypeTable -- st = slave table
				SELECT rn, ColumnName ,fk_IsGetDateColumnName, fk_DATA_TYPE
				  ,fk_ColumnName as fk_ColumnName
				  ,rn_ord as rn_ord
				  ,rn_join as rn_join
				  ,'st' as TypeTable -- st = slave table
				FROM steJoinColumns
						
				SELECT 
				  @JoinColumns = ( SELECT DISTINCT
									  ',' +
									  CASE WHEN t2.IsGetDateColumnName = 1 --CharIndex( fk_ColumnName + ',' , @GetDateColumns + ',') > 0
										THEN
										  'CASE WHEN st' + CAST (t2.rn_join as varchar(10)) + '.[' + fk_ColumnName + '] IS NOT NULL THEN ''1975-07-14'' ELSE ''1900-01-01'' END ' + fk_ColumnName + CAST (t2.rn_join as varchar(10))
										ELSE
											CASE WHEN DATA_TYPE IN ('char','nchar','varchar','nvarchar','sysname','text','ntext')
											THEN
												CASE WHEN @UseCollation = 1
													THEN
														'st' + CAST (t2.rn_join as varchar(10)) + '.[' + fk_ColumnName + '] COLLATE DATABASE_DEFAULT ' + fk_ColumnName + CAST (t2.rn_join as varchar(10))
													ELSE
														'st' + CAST (t2.rn_join as varchar(10)) + '.[' + fk_ColumnName + '] ' + fk_ColumnName + CAST (t2.rn_join as varchar(10)) -- COLLATE DATABASE_DEFAULT [' + fk_ColumnName + ']'
												END
											ELSE
												'st' + CAST (t2.rn_join as varchar(10)) + '.[' + fk_ColumnName + '] ' + fk_ColumnName + CAST (t2.rn_join as varchar(10))
											END
									  END
									  as 'data()' 
									FROM 
									  #ColumnsList t2 
									WHERE 
									  t1.rn = t2.rn 
									AND t2.TypeTable = 'st'
									FOR xml PATH('') 
								   )
				 ,@JoinColumnsExcludedDeletedDateColumns = ( SELECT 
														 '[' + fk_ColumnName + CAST (t2.rn_join as varchar(10)) + '],'  as 'data()' 
												FROM 
												  #ColumnsList t2 
												WHERE 
												  t1.rn = t2.rn 
												  AND t2.TypeTable = 'st'
												FOR xml PATH('') 
											   )
				 ,@JoinTables =  ( SELECT DISTINCT
									  ' LEFT JOIN rplstr[' +
									  t3.fk_SchemaName + '].[' + t3.fk_TableName + '] st' + CAST (t2.rn_join as varchar(10)) 
									  + ' ON mt.[' + t3.ColumnName + '] = st' + CAST (t2.rn_join as varchar(10)) + '.[' + t3.fk_ColumnName + ']'
									  as 'data()' 
									FROM 
									  #ColumnsList t2 
									  LEFT JOIN #TableListFK t3 ON t3.rn_ord = t2.rn_ord
															   AND t3.GroupTable = @i
															   AND t3.IsFKtoRefIdentity = 1
															   AND t3.fk_IsIdentity = 1
									WHERE 
									  t1.rn = t2.rn 
									AND t2.TypeTable = 'st'
									FOR xml PATH('')   
								   )							  
				FROM 
				  #ColumnsList t1
				WHERE 
					t1.TypeTable = 'st'
				GROUP BY 
				  rn;	
		  
				IF @IsDebuging = 1  BEGIN
					SELECT @JoinColumns as '@JoinColumns', @JoinTables as '@JoinTables'
				END

				-- Reduce the lenght of builded columns
				-- remove follow replace if you got issues with date type DateTime2 or column names as reserved
				SET @JoinColumns = REPLACE (@JoinColumns,'[','')
				SET @JoinColumns = REPLACE (@JoinColumns,']','')
				IF @UseCollation <> 1 BEGIN
					SET @JoinColumns = REPLACE (@JoinColumns,'''1900-01-01''',0)
				END
			END;			
			----- end joins
			IF @IsDebuging = 1  BEGIN
				select '#ColumnsList' as TableName, * from #ColumnsList
			END			
			
			
			SET @Qry = ''

			SELECT @Columns = ( SELECT 
								  CASE WHEN t2.IsGetDateColumnName = 1 --CASE WHEN CharIndex( ColumnName + ',' , @DeletedDateColumns + ',') > 0 
									THEN 'CASE WHEN mt.[' + ColumnName + '] IS NOT NULL THEN ''1975-07-14'' ELSE ''1900-01-01'' END [' + ColumnName + '],'
									ELSE 
										CASE WHEN DATA_TYPE IN ('char','nchar','varchar','nvarchar','sysname','text','ntext')
										THEN
											CASE WHEN @UseCollation = 1
												THEN
													'mt.[' + ColumnName + '] COLLATE DATABASE_DEFAULT [' + ColumnName + '],'
												ELSE
													'mt.[' + ColumnName + '],' -- COLLATE DATABASE_DEFAULT [' + ColumnName + '],'
											END
										ELSE
											'mt.[' + ColumnName + '],'
										END
								  END as 'data()' 
								FROM 
								  #ColumnsList t2
								WHERE 
									t1.rn = t2.rn 
									AND t2.TypeTable = 'mt'
								FOR xml PATH('') 
							   )
				 ,@ColumnsExcludedDeletedDateColumns = ( SELECT 
										 '[' + ColumnName + '],'  as 'data()' 
								FROM 
								  #ColumnsList t2 
								WHERE 
								  t1.rn = t2.rn 
								  AND t2.TypeTable = 'mt'
								FOR xml PATH('') 
							   )
				  -- build order of columns
				 ,@ColumnsOrder = ( SELECT 
								  CAST (ROW_NUMBER() OVER(ORDER BY (select null)) as nvarchar(3))+ ',' as 'data()' 
								  FROM 
									(SELECT 1 as rn -- as was added dummy column "DiffType" then we need to add extra one column for order
									 UNION ALL 
									 SELECT t3.rn FROM #ColumnsList t3
									 WHERE t3.TypeTable = 'mt'
									 ) t2
								  WHERE 
									t1.rn=t2.rn 
								  FOR xml PATH('') 
							   ) 
			FROM 
				#ColumnsList t1
			WHERE 
				t1.TypeTable = 'mt'
			GROUP BY 
				rn;

			-- Reduce the lenght of builded columns
			-- remove follow replace if you got issues with date type DateTime2 or column names as reserved
			SET @Columns = REPLACE (@Columns,'[','')
			SET @Columns = REPLACE (@Columns,']','')
			IF @UseCollation <> 1 BEGIN
			  SET @Columns = REPLACE (@Columns,'''1900-01-01''',0)
			END

			-- remove last char
			IF lEN(@Columns) > 0 BEGIN
				SET @Columns = SUBSTRING(@Columns, 1, LEN(@Columns)-1);
				SET @ColumnsExcludedDeletedDateColumns = SUBSTRING(@ColumnsExcludedDeletedDateColumns, 1, LEN(@ColumnsExcludedDeletedDateColumns)-1);
				SET @JoinColumnsExcludedDeletedDateColumns = SUBSTRING(@JoinColumnsExcludedDeletedDateColumns, 1, LEN(@JoinColumnsExcludedDeletedDateColumns)-1);
			END ELSE BEGIN
				SET @Columns = Null; --@ZeroNullVal; 
				SET @ColumnsExcludedDeletedDateColumns = Null;
				SET @JoinColumnsExcludedDeletedDateColumns = Null;
				SET @JoinColumns = '';
				SET @JoinTables = '';
			END; 
			
			-- remove last char
			IF lEN(@ColumnsOrder) > 0 BEGIN
				SET @ColumnsOrder = SUBSTRING(@ColumnsOrder, 1, LEN(@ColumnsOrder)-1);
			END ELSE BEGIN
				SET @ColumnsOrder = 1; 
			END;
			
			SET @Columns = REPLACE (@Columns, ',', CHAR(13) + ',')
			SET @ColumnsExcludedDeletedDateColumns = REPLACE (@ColumnsExcludedDeletedDateColumns, ',', CHAR(13) + ',')
			SET @JoinColumnsExcludedDeletedDateColumns = REPLACE (@JoinColumnsExcludedDeletedDateColumns, ',', CHAR(13) + ',')
			SET @JoinColumns = REPLACE (@JoinColumns, '))', '))' + CHAR(13))
			SET @JoinTables  = REPLACE (@JoinTables, 'LEFT JOIN', CHAR(13) + 'LEFT JOIN')
			--SELECT @Columns, @ColumnsOrder
			
			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT '@SchemaName' + ISNULL(@SchemaName,'NULL');
				PRINT '@TableName' + ISNULL(@TableName,'NULL');
				PRINT '@Columns: ' + ISNULL(@Columns,'NULL');
				PRINT '@ColumnsExcludedDeletedDateColumns: ' + ISNULL(@ColumnsExcludedDeletedDateColumns,'NULL');
				PRINT '@JoinColumnsExcludedDeletedDateColumns: ' + ISNULL(@JoinColumnsExcludedDeletedDateColumns,'NULL');
				PRINT '@JoinColumns:' + ISNULL(@JoinColumns,'NULL')
			END
			
			IF @IsDebuging = 1
				SELECT @Columns AS [Selected Columns List];
						
			-- Except A vs BL
			SET @OutputIntoTableSelect = 'SELECT '
										+ CHAR(13) + ' ''In Act - Not In BL'' as DiffType, * ' --AvsBL
										+ CHAR(13) + 'INTO TestCHK_SP_' + @SchemaName + @TableName + ' FROM (' 
										+ CHAR(13)
			
			SET @Qry = 
					 @OutputIntoTableSelect
					 + ' SELECT '
					 + CHAR(13) + '' + COALESCE(@Columns,@ZeroNullVal + ' col1') 
					 --+ CHAR(13) + ','  + @ZeroNullVal 
					 + CHAR(13) + '' + @JoinColumns
					 + CHAR(13) + 'FROM' 
					 + CHAR(13)
					 + '' + @DBActual + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					 + CHAR(13) + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBActual + '.[')
					 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
					 + CHAR(13) + 'EXCEPT' + CHAR(13)
					 + 'SELECT'
					 + CHAR(13) + ' ' + COALESCE(@Columns,@ZeroNullVal + ' col1') 
					 --+ CHAR(13) + ','  + @ZeroNullVal 
					 + CHAR(13) + '' + @JoinColumns
					 + CHAR(13) + 'FROM' 
					 + CHAR(13)
					 + '  ' + @DBBaseLine + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					 + CHAR(13) + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBBaseLine + '.[')
					 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
					 + ') t '
					 + CHAR(13) + 'ORDER BY ' + @ColumnsOrder
					 -- + CHAR(13) + ' OPTION ( HASH  JOIN, RECOMPILE )'
			
			SET @QryInfo = 'SELECT ''[' + @SchemaName + '].[' + @TableName + ']''' + ' [Result for:]'

			IF @IsDebuging = 1
				EXEC (@QryInfo);
			
			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT IsNull(@Qry,'null script')
			END

			EXEC (@Qry); -- replaced with sp_executesql because TOP was causing performance issue Google
			--EXEC sp_executesql @Qry, N'@Top int', @Top = @Top

			SET @ExceptAvsBL = @@ROWCOUNT;
						
			-- Except BL vs A
			SET @OutputIntoTableSelect = 'INSERT INTO TestCHK_SP_' + @SchemaName + @TableName 
										+ CHAR(13) + ' SELECT '
										--+ CHAR(13) + ' SELECT TOP (@Top) '
										+ CHAR(13) + ' ''In BL - Not In Act'' DiffType, ' --BLvsA
										+ ' * FROM (' 
										+ CHAR(13)
						
			SET @Qry = 
					 @OutputIntoTableSelect
					 + ' SELECT '
					 + CHAR(13) + '' + COALESCE(@Columns,@ZeroNullVal + ' col1') 
					 --+ CHAR(13) + ','  + @ZeroNullVal
					 + CHAR(13) + '' + @JoinColumns
					 + CHAR(13) + 'FROM' 
					 + CHAR(13)
					 + '' + @DBBaseLine + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					 + CHAR(13) + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBBaseLine + '.[')
					 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
					 + CHAR(13) + 'EXCEPT' + CHAR(13)
					 + 'SELECT'
					 + CHAR(13) + '' + COALESCE(@Columns,@ZeroNullVal + ' col1') 
					 --+ CHAR(13) + ','  + @ZeroNullVal 
					 + CHAR(13) + '' + @JoinColumns
					 + CHAR(13) + 'FROM' 
					 + CHAR(13)
					 + '' + @DBActual + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					 + CHAR(13) + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBActual + '.[')
					 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
					 + ') t '
					 + CHAR(13) + 'ORDER BY ' + @ColumnsOrder
					-- + CHAR(13) + ' OPTION ( HASH  JOIN , RECOMPILE)'
			EXEC (@Qry);
			--EXEC sp_executesql @Qry, N'@Top int', @Top = @Top
			SET @ExceptBLvsA = @@ROWCOUNT;
			
			-- return diffs
			SET @Qry = 'SELECT TOP (' + CAST(@Top AS NVARCHAR(10)) + ')'
					+ ' * FROM TestCHK_SP_' + @SchemaName + @TableName
					+ ' ORDER BY ' + COALESCE(NULLIF(@OrderByColumn,''),'2') + ',1'

			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT @Qry
			END
			EXEC (@Qry);

			-- ************************************
			-- START Detailed DIffs and (%)
			-- ************************************
			/*
			my crazy formula to handle diffs and division to Zero (redbull issue)
			note: use Decimal or Float type otherwise small result :) will be rounded to zero
			Select (COALESCE(NullIf(@iAct,100),@iTotal) -- Set Actual and set equal to Total if is 0
				   * 100/
				   (COALESCE(NullIf(@iTotal,0),COALESCE(NullIf(@iAct,0),1))) -- Set Total and set equal to Actual if is 0 and handle division to 0
				   )			 
			*/
			-- if is no columns for diff then get counts
			IF IsNUll(@Columns,'1') = '1' BEGIN
				-- count from A 
				SET @Qry = 
						 ' SELECT ' 
						 + CHAR(13) + '@ExceptAvsBL = COUNT (*)'  
						 + CHAR(13) + ' FROM ' 
						 + CHAR(13)
						 + '  ' + @DBActual + '.[' + @SchemaName + '].[' + @TableName + ']'
						 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
				exec sp_executesql @Qry, N'@ExceptAvsBL int out', @ExceptAvsBL out
				--select @ExceptAvsBL as ExceptAvsBL
				-- count from BL
				SET @Qry = 
						 ' SELECT ' 
						 + CHAR(13) + '@ExceptBLvsA = COUNT (*)'  
						 + CHAR(13) + ' FROM ' 
						 + CHAR(13)
						 + '  ' + @DBBaseLine + '.[' + @SchemaName + '].[' + @TableName + ']'
						 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
				exec sp_executesql @Qry, N'@ExceptBLvsA int out', @ExceptBLvsA out
				--select @ExceptBLvsA as ExceptBLvsA
				SET @ExceptBLvsA = @ExceptBLvsA - @ExceptAvsBL
				SET @ExceptAvsBL = 0  -- set to zero because Main count (see next query) is always equal with this variable
			END		   


			-- Build Tables if not exist
			IF OBJECT_ID('tempdb..#testRunnerResultsCHK') IS NOT NULL DROP TABLE testRunnerResultsCHK
			CREATE TABLE #testRunnerResultsCHK(
				[Test Start Time] datetime NOT NULL,
				[TableName] [nvarchar](256) COLLATE DATABASE_DEFAULT NOT NULL,
				[CNT Total] [bigint] NULL,
				[CNT Diff A vs BL] [bigint] NULL,
				[CNT Diff BL vs A] [bigint] NULL,
				[Diff BL vs Total %] [numeric](18, 12) NULL,
				[Diff A vs Total %] [numeric](18, 12) NULL,
				[Diff BL and A vs Total %] [numeric](18, 12) NULL
			) ON [PRIMARY]

			
			SET @OutputIntoTableSelect = 'INSERT INTO #testRunnerResultsCHK SELECT ''' + CONVERT(varchar(20), @ExecDate, 20) 
										 + ''' as [Test Start Time], * FROM (' + CHAR(13)

			 
			SET @Qry = 
					 @OutputIntoTableSelect
					 + ' SELECT ' 
					 + CHAR(13) + ' ''' + @SchemaName + '.' + @TableName + '''  TableName ' 
					 + CHAR(13) + ', COUNT(*) AS [CNT Total]'
					 + CHAR(13) + ',' + Cast(@ExceptAvsBL as varchar(20)) + ' [CNT Diff A vs BL]'
					 + CHAR(13) + ',' + Cast(@ExceptBLvsA as varchar(20)) + ' [CNT Diff BL vs A]'
					 -- % calcs
					 + CHAR(13) + ',(IsNUll(NullIf(cast(' + Cast(@ExceptAvsBL as varchar(20)) + ' as decimal),100),COUNT(*)) * 100/(IsNUll(NullIf(COUNT(*),0),IsNUll(NullIf(cast(' + Cast(@ExceptAvsBL as varchar(20)) + ' as decimal),0),1)))) [Diff BL vs Total %]'
					 + CHAR(13) + ',(IsNUll(NullIf(cast(' + Cast(@ExceptBLvsA as varchar(20)) + ' as decimal),100),COUNT(*)) * 100/(IsNUll(NullIf(COUNT(*),0),IsNUll(NullIf(cast(' + Cast(@ExceptBLvsA as varchar(20)) + ' as decimal),0),1)))) [Diff A vs Total %]'
					 + CHAR(13) + ',(IsNUll(NullIf(cast(' + Cast(@ExceptAvsBL + @ExceptBLvsA as varchar(20)) + ' as decimal),100),COUNT(*)) * 100/(IsNUll(NullIf(COUNT(*),0),IsNUll(NullIf(cast(' + Cast(@ExceptAvsBL + @ExceptBLvsA as varchar(20)) + ' as decimal),0),1)))) [Diff BL and A vs Total %]'					 
					 --
					 + CHAR(13) + ' FROM ' 
					 + CHAR(13)
					 + '  ' + @DBActual + '.[' + @SchemaName + '].[' + @TableName + ']'
					 + CHAR(13) + COALESCE('WHERE 1 = 1 ' + REPLACE(@WhereClause,'mt.',''),'')
					 + ') t'
			IF @DetailedPrint = 1 BEGIN
				PRINT ''		 
				PRINT @Qry;					 
			END
			EXEC (@Qry);

			SET @Qry = 'SELECT * FROM #testRunnerResultsCHK'
			IF (@DetailedOutput = 1)
			BEGIN
				EXEC (@Qry);
			END
			
			
			-- ************************************
			-- END Detailed DIffs
			-- ************************************
			
			-- ************************************
			-- START Diff Columns 
			-- find Columns difs, be prepared for screw your mind :)
			-- ************************************
			--
			-- select * from #ColumnsList
			-- create temp table, reusing existing vars , sorry
			SELECT @ColumnsCHK = null 
			SELECT @ColumnsCHK =  COALESCE(@ColumnsCHK + ' bigint,' + char(13) ,'rn int,') + RTRIM(cl.ColumnName)
			FROM #ColumnsList cl 
			WHERE cl.TypeTable = 'mt'
			ORDER BY cl.ColumnName

			SELECT @ColumnsCHK =  COALESCE(@ColumnsCHK + ' bigint,' + char(13) ,'rn int,') + RTRIM(cl.fk_ColumnName) + CAST(cl.rn_join AS VARCHAR(10))
			FROM #ColumnsList cl 
			WHERE cl.TypeTable = 'st'
			ORDER BY cl.ColumnName

			
			-- *** Stage #1 prepare data calculate checksum per column 
			-- use global table as cant create dynamical in same scope temp table
			IF OBJECT_ID('tempdb..##DiffColumns') IS NOT NULL DROP TABLE ##DiffColumns 
			SET @Qry = 'CREATE TABLE ##DiffColumns (' + COALESCE(@ColumnsCHK, 'rn') + ' bigint )'
			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT @Qry
			END
			EXEC SP_EXECUTESQL @Qry			
			
			-- build columns with CHK 
			SELECT @ColumnsCHK = null
			
			--SELECT @ColumnsCHK =  COALESCE(@ColumnsCHK + ',' + char(13) ,'') + 'ABS(CONVERT(BIGINT,CHECKSUM_AGG(CHECKSUM(' + RTRIM(cl.ColumnName) + ')))) as ' +RTRIM(cl.ColumnName)
			-- as we know issue with CHECKSUM_AGG, used SUM of CHECKSUMs instead of
			SELECT @ColumnsCHK =  COALESCE(@ColumnsCHK + ',' + char(13) ,'') + 'SUM(CONVERT(BIGINT,CHECKSUM(mt.' + RTRIM(cl.ColumnName) + '))) ' +RTRIM(cl.ColumnName)
			FROM #ColumnsList cl 
			WHERE cl.TypeTable = 'mt'
			ORDER BY cl.ColumnName
			
			IF (@JoinFKTables = 1) BEGIN
				SELECT @ColumnsCHK =  COALESCE(@ColumnsCHK + ',' + char(13) ,'') + 'SUM(CONVERT(BIGINT,CHECKSUM(st' + CAST(cl.rn_join AS VARCHAR(10))+ '.' + RTRIM(cl.fk_ColumnName) + '))) ' +RTRIM(cl.fk_ColumnName)+CAST(cl.rn_join AS VARCHAR(10))
				FROM #ColumnsList cl 
				WHERE cl.TypeTable = 'st'
				ORDER BY cl.ColumnName
			END
			
			--PRINT @ColumnsCHK
			SET @Qry = 
					  ' SELECT 1 as rn,' -- Actual (A)
					  + CHAR(13)
					  + @ColumnsCHK
					  + CHAR(13) + 'FROM'
					  + CHAR(13)
					  + '' + @DBActual + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					  
			IF (@JoinFKTables = 1) BEGIN
				SET @Qry = @Qry + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBActual + '.[')
			END;
			SET @Qry = @Qry + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
			
			IF @DetailedPrint = 1 BEGIN
				PRINT ''		  
				PRINT '-- Actual (A)' + @Qry
			END
			--EXEC (@Qry); 
		   
			---- insert Actual data into
			INSERT ##DiffColumns EXEC (@Qry)
			
			SET @Qry = 
					  ' SELECT 2 as rn,' -- BAseLine (BL)
					  + CHAR(13)
					  + @ColumnsCHK
					  + CHAR(13) + 'FROM' 
					  + CHAR(13)
					  + '' + @DBBaseLine + '.[' + @SchemaName + '].[' + @TableName + '] mt'
					  
			IF (@JoinFKTables = 1) BEGIN
				SET @Qry = @Qry + REPLACE (@JoinTables, 'JOIN rplstr[', 'JOIN ' + @DBBaseLine + '.[')
			END;
			SET @Qry = @Qry + CHAR(13) + COALESCE('WHERE 1 = 1 ' + @WhereClause,'')
			
			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT '--  BAseLine (BL)' +  @Qry
			END
			--EXEC (@Qry);
			---- insert Base Line data into
			INSERT ##DiffColumns EXEC (@Qry)
			--select * from ##DiffColumns
						
			-- *** Stage #2 find Diffs between columns of A vs BL
			IF OBJECT_ID('tempdb..##DiffColumnsChanged') IS NOT NULL DROP TABLE ##DiffColumnsChanged
			SET @Qry = 'CREATE TABLE ##DiffColumnsChanged (TableName nvarchar(265) COLLATE DATABASE_DEFAULT, ColName nvarchar(128) COLLATE DATABASE_DEFAULT, CHK_A bigint, CHK_BL bigint )'
			EXEC SP_EXECUTESQL @Qry
			
			/*
			-- Build Tables if not exist
			IF OBJECT_ID('tempdb..#testRunnerResultsColumnsCHK') IS NOT NULL DROP TABLE #testRunnerResultsColumnsCHK
			CREATE TABLE #testRunnerResultsColumnsCHK(
					[Test Start Time] datetime NOT NULL,
					[TableName] [nvarchar](128) COLLATE DATABASE_DEFAULT NOT NULL,
					[ColName] [nvarchar](128) COLLATE DATABASE_DEFAULT NOT NULL,
					[CHK_A] [bigint] NOT NULL,
					[CHK_BL] [bigint] NOT NULL
				) ON [PRIMARY]
			*/

			SET @OutputIntoTableSelect = 'SELECT * FROM ('

			SET @Qry = 
					  ' ;with cteUnpivot as ( ' + CHAR(13)
					+ ' select l.*, r.CHK_BL '+ CHAR(13)
					+ ' from '+ CHAR(13)
					+ '( '+ CHAR(13)
					+ '	select unp1.ColName, unp1.CHK_A from ##DiffColumns t1 '+ CHAR(13)
					+ '	unpivot (CHK_A for ColName in ( ' + @ColumnsExcludedDeletedDateColumns + CHAR(13) 
					+ ',' + @JoinColumnsExcludedDeletedDateColumns
					+ CHAR(13)
					+ '	   )) unp1 '+ CHAR(13)
					+ '	where unp1.rn = 1 '+ CHAR(13)
					+ ') l '+ CHAR(13)
					+ 'left join '+ CHAR(13)
					+ '( '+ CHAR(13)
					+ '	select unp2.ColName, unp2.CHK_BL from ##DiffColumns t2 '+ CHAR(13)
					+ '	unpivot (CHK_BL for ColName in ( ' + @ColumnsExcludedDeletedDateColumns + CHAR(13) 
					+ ',' + @JoinColumnsExcludedDeletedDateColumns
					+ CHAR(13)
					+ '	   )) unp2 '+ CHAR(13)
					+ '	where unp2.rn = 2 '+ CHAR(13)
					+ ') r on r.ColName = l.ColName '+ CHAR(13)
					+ ') '+ CHAR(13)
					+ @OutputIntoTableSelect
					+ 'select '
					+ CHAR(13) + ' ''' + @SchemaName + '.' + @TableName + ''' TableName, ' 
					+ ' un.ColName, un.CHK_A, un.CHK_BL '+ CHAR(13)
					+ 'from cteUnpivot un '+ CHAR(13)
					+ 'where un.CHK_A <> un.CHK_BL '+ CHAR(13)
					+ ')t'

			IF @DetailedPrint = 1 BEGIN
				PRINT ''
				PRINT @Qry
			END

			IF (@DetailedOutput = 1)
			BEGIN
				---- insert Actual data into
				INSERT ##DiffColumnsChanged EXEC (@Qry)
			
				-- return difs of changed columns
				SELECT  * FROM ##DiffColumnsChanged
			
				SET @ColumnsChanged = null
				SELECT @ColumnsChanged =  COALESCE(@ColumnsChanged + ', '  ,'') + RTRIM(cc.ColName)
				FROM ##DiffColumnsChanged cc
			
				IF (SELECT COUNT(*) FROM #ColumnsList cl
					WHERE cl.TypeTable = 'mt'
					) 
				  = (SELECT COUNT(*) FROM ##DiffColumnsChanged)  BEGIN
				   SET @ColumnsChanged = N'Everyone and his dog : ' + @ColumnsChanged
				END;			 
			
				SELECT @ColumnsChanged as ColumnsChanged
			END
						
			DROP TABLE ##DiffColumns  
			DROP TABLE ##DiffColumnsChanged
			-- ************************************
			-- END Diff Columns 
			-- ************************************  

			-- Continue next code from this point (.) 
			
		END; -- IF
	  END; -- Loop While
	  
END;