/*
USE
CALL bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	v_start 	  timestamptz;
	v_end 		  timestamptz;
	v_batch_start timestamptz;
	v_batch_end   timestamptz;

	secs		   INTEGER;

BEGIN

	v_batch_start := clock_timestamp();

	RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '================================================';

	-- crm_cust_info
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_cust_info';
	COPY bronze.crm_cust_info
	FROM 'D:\prog2\pet_sql\datasets\source_crm\cust_info.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

	-- crm_prd_info
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_prd_info';
	COPY bronze.crm_prd_info
	FROM 'D:\prog2\pet_sql\datasets\source_crm\prd_info.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

	-- crm_sales_details
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.crm_sales_details';
	COPY bronze.crm_sales_details
	FROM 'D:\prog2\pet_sql\datasets\source_crm\sales_details.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';
	
	-- erp_loc_a101
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101
	FROM 'D:\prog2\pet_sql\datasets\source_erp\loc_a101.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

	-- erp_cust_az12
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
	FROM 'D:\prog2\pet_sql\datasets\source_erp\cust_az12.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

	-- erp_px_cat_g1v2
	v_start := clock_timestamp();
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	RAISE NOTICE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
	FROM 'D:\prog2\pet_sql\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMiTER ','
	);

	v_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_end - v_start));
	RAISE NOTICE 'Load duration: %', secs;
	RAISE NOTICE '================================================';

	v_batch_end := clock_timestamp();
	secs := EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', secs;
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        DECLARE
            err_state   text;
            err_msg     text;
            err_detail  text;
            err_hint    text;
            err_ctx     text;
        BEGIN
            GET STACKED DIAGNOSTICS
                err_state  = RETURNED_SQLSTATE,
                err_msg    = MESSAGE_TEXT,
                err_detail = PG_EXCEPTION_DETAIL,
                err_hint   = PG_EXCEPTION_HINT,
                err_ctx    = PG_EXCEPTION_CONTEXT;

            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error SQLSTATE: %', err_state;
            RAISE NOTICE 'Error Message : %', err_msg;
            IF err_detail IS NOT NULL THEN RAISE NOTICE 'Detail       : %', err_detail; END IF;
            IF err_hint   IS NOT NULL THEN RAISE NOTICE 'Hint         : %', err_hint;   END IF;
            IF err_ctx    IS NOT NULL THEN RAISE NOTICE 'Context      : %', err_ctx;    END IF;
            RAISE NOTICE '==========================================';

            RAISE; -- rethrow if you want the CALL to fail
        END;
END;
$$;
