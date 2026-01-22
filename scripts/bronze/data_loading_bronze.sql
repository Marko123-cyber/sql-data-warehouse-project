CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    BEGIN TRY
        DECLARE 
            @total_start_time DATETIME2, 
            @total_end_time DATETIME2, 
            @table_start_time DATETIME2, 
            @table_end_time DATETIME2,
            @msg NVARCHAR(200),
            @row_count INT; 
        
        SET @total_start_time = GETDATE(); 
        PRINT '========================================';
        PRINT 'Starting Bronze Layer Data Load Process';
        PRINT 'Start Time: ' + CONVERT(NVARCHAR, @total_start_time, 120);
        PRINT '========================================';
        PRINT '';
        
        -- CRM CUST INFO
        PRINT '--- Loading CRM Customer Info ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT 'Inserting data into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for crm_cust_info: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- CRM PRD INFO
        PRINT '--- Loading CRM Product Info ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'Inserting data into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for crm_prd_info: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- CRM SALES DETAILS
        PRINT '--- Loading CRM Sales Details ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'Inserting data into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for crm_sales_details: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- ERP CUST
        PRINT '--- Loading ERP Customer Data ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'Inserting data into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for erp_cust_az12: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- ERP LOC
        PRINT '--- Loading ERP Location Data ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'Inserting data into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for erp_loc_a101: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- ERP PX CAT
        PRINT '--- Loading ERP Product Category Data ---';
        SET @table_start_time = GETDATE();
        PRINT 'Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'Inserting data into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/dwh_project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @row_count = @@ROWCOUNT;
        SET @table_end_time = GETDATE();
        PRINT 'Rows inserted: ' + CAST(@row_count AS NVARCHAR);
        SET @msg = 'Loading time for erp_px_cat_g1v2: ' 
            + CAST(DATEDIFF(MILLISECOND, @table_start_time, @table_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '';
        
        -- TOTAL TIME
        SET @total_end_time = GETDATE(); 
        PRINT '========================================';
        PRINT 'Bronze Layer Data Load Process Completed';
        PRINT 'End Time: ' + CONVERT(NVARCHAR, @total_end_time, 120);
        SET @msg = 'Total loading time: ' 
            + CAST(DATEDIFF(MILLISECOND, @total_start_time, @total_end_time) AS NVARCHAR) 
            + ' ms';
        PRINT @msg;
        PRINT '========================================';
    END TRY
    BEGIN CATCH
        PRINT '=====================';
        PRINT 'Error occurred during loading bronze layer';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=====================';
    END CATCH

END;
GO
USE DataWarehouse;
GO
EXEC bronze.load_bronze;
