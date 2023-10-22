
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/* 
	This procedure creates a staging table dbo.ProductionDim, by joining columns from from dbo.ProductInfo and dbo.WarehouseInfo
	this enables us to apply the business logic in the Production Deficiency Report to the historical data we aim to collect. 
	
*/

-- Create the procedure 
ALTER PROCEDURE dbo.ProductionHistory AS

BEGIN

	SET NOCOUNT ON;

	----Creates the staging table dbo.ProductionDim for the first time

	IF OBJECT_ID('dbo.ProductionDim', 'U') IS NULL

	BEGIN

		CREATE TABLE dbo.CloudServerDim
	
			(
			ProductId						NVARCHAR(100),
			ProductionId					UNIQUEIDENTIFIER,
			ProductName						NVARCHAR(200),
			Category						NVARCHAR(200),
			WarehouseId						VARCHAR(100),
			Region							VARCHAR(100),
			Warehouse					    NVARCHAR(100),
			DeficiencyStatus				VARCHAR(50)
			)

	
	END

	---Prepare table for Load

	TRUNCATE TABLE dbo.ProductionDim


	INSERT INTO dbo.ProductionDim
	
	SELECT 
		i.Id AS	ProductId,
		i.ProductionId AS ProductionId,
		i.ProductName AS ProductName,
		i.Category AS Category,
		w.WarehouseId AS WarehouseId,
		l.Region AS Region,
		w.Warehouse AS Warehouse,
		i.DeficiencyStatus as DeficiencyStatus
	FROM	
	dbo.ProductInfo as i
	Join dbo.WarehouseInfo as w
	on i.ProductId = w.ProductId
	join dbo.Locations as l
	on w.WarehouseId = l.WarehouseId


----Creates the Temporal table dbo.ApplicationServerTracker for the first time
	
	IF OBJECT_ID('dbo.ProductionTracker', 'U') IS NULL
	BEGIN
		CREATE TABLE dbo.ProductionTracker
	
			(
			ProductId						NVARCHAR(100) PRIMARY KEY,
			ProductionId					NVARCHAR(200),
			ProductName						NVARCHAR(200),
			Category						NVARCHAR(200),
			WarehouseId						VARCHAR(100),
			Region							VARCHAR(50),
			Warehouse						VARCHAR(100),
			DeficiencyStatus				VARCHAR(50),
			SysStartTime					DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
			SysEndTime						DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
			PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)

			)
			WITH
			(
				SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.ProductionHistory)
			)


	INSERT INTO dbo.ProductionTracker
	(
		ProductId,
		ProductionId,
		Productname,
		Category,
		WarehouseId,
		Region,
		Warehouse,
		DeficiencyStatus

	)

	---- Load table for the first time

	
	SELECT 
		ProductId,
		ProductionId,
		Productname,
		Category,
		WarehouseId,
		Region,
		Warehouse,
		DeficiencyStatus
	FROM dbo.ProductionDim


	END
		
	-- We will then use Merge to update the table
	
	MERGE dbo.ProductionTracker AS dst
	USING  dbo.ProductionDim AS src
	ON (src.ProductId = dst.ProductId)


	WHEN MATCHED AND 
		dst.DeficiencyStatus <> Deficiency.RiskStatus
		


	THEN UPDATE
	
	SET dst.ProductionId = src.ProductionId,
		dst.ProductName = src.ProductName,
		dst.Category = src.Category,
		dst.WarehouseId = src.WarehouseId,
		dst.Region = src.Region,
		dst.Warehouse = src.Warehouse,
		dst.DeficiencyStatus = src.DeficiencyStatus
		


--- When not matched then insert values from source

	WHEN NOT MATCHED BY TARGET

	THEN

		INSERT(
			ProductId,
			ProductionId,
			Productname,
			Category,
			WarehouseId,
			Region,
			Warehouse,
			DeficiencyStatus
			)

		VALUES(
			src.ProductId,
			src.ProductionId,
			src.ProductName,
			src.Category,
			src.WarehouseId,
			src.Region,
			src.Warehouse,
			src.DeficiencyStatus
			)

	WHEN NOT MATCHED BY SOURCE

	THEN 
	
		DELETE ;
	

END