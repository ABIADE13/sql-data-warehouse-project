-- DROP PROCEDURE bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @starttime DATETIME, @endtime DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time = GETDATE();
PRINT '========================================';
PRINT '-----------LOAD BRONZE LAYER------------';
PRINT '========================================';

SET @starttime = GETDATE()
PRINT '<<<<<<<TRUNCATE TABLE CRM_CUST_INFO>>>>>';
TRUNCATE TABLE bronze.crm_cust_info;
PRINT '-----------------------------------------';
PRINT '<<<<<LOAD DATA INTO CRM_CUST_INFO>>>>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.crm_cust_info
from 'D:\DatawithBaraaSql\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@starttime, @endtime) AS NVARCHAR) + 'SECOND';
PRINT '>> ---------';

--SELECT * FROM bronze.crm_cust_info;
SET @starttime = GETDATE();
PRINT '<<<<<<<TRUNCATE TABLE CRM_PRD_INFO>>>>>';
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '-----------------------------------------';
PRINT '<<<<<LOAD DATA INTO CRM_PRD_INFO>>>>>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.crm_prd_info
FROM 'D:\DatawithBaraaSql\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR) + 'SECONDS';
PRINT '>>-----';

--SELECT * FROM bronze.crm_prd_info;
SET @starttime = GETDATE();
PRINT '<<<<<<<TRUNCATE TABLE CRM_SALES_DETAILS>>>>>';
TRUNCATE TABLE bronze.crm_sales_details;
PRINT '-----------------------------------------';
PRINT '<<<<LOAD DATA INTO CRM_SALES_DETAILS>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.crm_sales_details
FROM 'D:\DatawithBaraaSql\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>>LOADING DURATION: ' + CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR) + 'SECONDS';
PRINT '>>>------';


--SELECT * FROM bronze.crm_sales_details;
SET @starttime = GETDATE();
PRINT '<<<<<<<TRUNCATE TABLE ERP_CUST_AZ12>>>>>';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '-----------------------------------------';
PRINT '<<<<<LOAD DATA INTO ERP_CUST_AZ12>>>>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.erp_cust_az12
from 'D:\DatawithBaraaSql\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>>LOADING DURATION: ' + CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR) + 'SECONDS';

--SELECT * FROM bronze.erp_cust_az12;
SET @starttime = GETDATE();
PRINT '<<<<<<<TRUNCATE TABLE ERP_LOC_A101>>>>>';
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT '-----------------------------------------';
PRINT '<<<<<<LOAD DATA INTO ERP_LOC_A101>>>>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.erp_loc_a101
FROM 'D:\DatawithBaraaSql\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>>LOADING DAURATION: ' + CAST(DATEDIFF(second,@starttime,@endtime) as NVARCHAR) + 'SECONDS';
PRINT '>>--------';

--SELECT * FROM bronze.erp_loc_a101;
SET @starttime = GETDATE();
PRINT '<<<<<<<TRUNCATE TABLE ERP_PX_CAT_G1V2>>>>>';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '-----------------------------------------';
PRINT '<<<<<LOAD DATA INTO ERP_PX_CAT_G1V2>>>>>>';
PRINT '-----------------------------------------';
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\DatawithBaraaSql\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @endtime = GETDATE();
PRINT '>>LOADING DURATION: ' + CAST(DATEDIFF(second,@starttime,@endtime) as NVARCHAR) + 'SECONDS';
PRINT '>>-------';
SET @batch_end_time = GETDATE();
PRINT'>>>FULL BATCH LOADING DURATION';
PRINT '>>LOADING DURATION: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'SECONDS';
END TRY
BEGIN CATCH
	PRINT '==============================================';
	PRINT    'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT '==============================================';
END CATCH
END
--SELECT * FROM bronze.erp_px_cat_g1v2;
