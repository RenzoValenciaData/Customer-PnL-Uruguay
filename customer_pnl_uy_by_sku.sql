DECLARE Periodo_ini DATE DEFAULT '2023-01-01';
DECLARE Periodo_fin DATE DEFAULT '2024-12-31';
DECLARE Periodo_ini_fit DATE DEFAULT '2023-01-01';
DECLARE Company_code STRING DEFAULT 'UY02';
DECLARE Fit_entity STRING DEFAULT 'UYLFS';
DECLARE Param_customer STRING DEFAULT '#';
DECLARE Param_sku STRING DEFAULT '001';


CREATE OR REPLACE TABLE `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.t_fct_customer_pnl_uy_sku`  AS (


  /*===== DATA DE COPA BY SKU =========*/
WITH TOTAL_LA_COPA_DATA_BY_SKU AS (
    SELECT
      fiscper
      ,comp_code
      ,LTRIM(material,"0") AS SKU
      ,customer
      ,doc_number
  ,CASE
      WHEN COMP_CODE = "AR02" THEN "ARS"
      WHEN COMP_CODE = "BO03" THEN "BOB_ME"
      WHEN COMP_CODE = "BR02" THEN "BRL"
      WHEN COMP_CODE = "BR04" THEN "BRL"
      WHEN COMP_CODE = "CL02" THEN "CLP"
      WHEN COMP_CODE = "CO05" THEN "COP_ME"
      WHEN COMP_CODE = "CR02" THEN "CRC"
      WHEN COMP_CODE = "DO03" THEN "DOP"
      WHEN COMP_CODE = "EC02" THEN "ECS"
      WHEN COMP_CODE = "GT02" THEN "GTQ"
      WHEN COMP_CODE = "HN02" THEN "HNL"
      WHEN COMP_CODE = "MX02" THEN "MXN"
      WHEN COMP_CODE = "NI02" THEN "NIO"
      WHEN COMP_CODE = "PA02" THEN "PAB"
      WHEN COMP_CODE = "PE02" THEN "PEN"
      WHEN COMP_CODE = "PR04" THEN "USD"
      WHEN COMP_CODE = "SV02" THEN "SVC"
      WHEN COMP_CODE = "UY02" THEN "UYU"
  END AS CURRENCY_COPA
      ,SUM(g_qvv003 ) AS Volume
      ,SUM(
        CASE
          WHEN comp_code = "CL02" THEN 100 * g_avv004
          WHEN comp_code IN ("BR02", "BR04") AND biczww154 = "GS" THEN g_avv004
          WHEN comp_code IN ("BR02", "BR04") AND biczww154 = "UD" THEN 0
          ELSE g_avv004
        END) AS Gross_Sales
  
      ,SUM(IF(comp_code = "CL02",100* g_avv158 , g_avv158 )) AS Trade_Incentives
      ,SUM(IF(comp_code = "CL02",100* g_avv157 , g_avv157 )) AS Consumer_Incentives
      ,SUM(IF(comp_code = "CL02",100* g_avv028 , g_avv028 )) AS Other_Deductions
      ,SUM(
        CASE
          WHEN comp_code = "CL02" THEN 100 * g_avv150  + g_avv105  
          WHEN comp_code IN ("BR02", "BR04") AND biczww154 = "GS" THEN g_avv150 + g_avv105
          WHEN comp_code IN ("BR02", "BR04") AND biczww154 = "UD" THEN g_avv004 + g_avv105
          ELSE g_avv150  + g_avv105
        END) AS SALES_ALLOW_RETURNS
  
  ,SUM(IF(comp_code = "CL02",100* g_avv159 , g_avv159 )) AS NPD
  ,(sum(G_AVV038) + sum(G_AVV070) + sum(G_AVV073) + sum(G_AVV095) + sum(G_AVV096)+ sum(G_AVV104) + sum(G_AVV151) + sum(BICZVV166)) as COGS_MANUFACTURING
  ,(sum(G_AVV153) + sum(G_AVV152) + sum(BICZVV190) + sum(BICZVV189)) COGS_LOGISTIC
  FROM `prd-amer-analyt-datal-svc-88.amer_t_etl_stg.t_copa_line_items_la_tmp`
    WHERE pstng_date between Periodo_ini AND Periodo_fin
      AND IF(COMP_CODE IN ("EC02","PR04","PA02","SV02"), CURRENCY = "USD", CURRENCY != "USD")
      AND BICZWW154 NOT IN ("IC")
      -- AND IF(fiscper = "2024003", OPFLAG is null, OPFLAG = "I")
      AND (BICZWW153 IS NULL OR BICZWW153 = "ES")
      /* --AND comp_code != "VE24"     REVISAR SI ES REQUERIDO*/
      AND comp_code= Company_code --SOLO URUGUAY
      --AND (CUSTOMER NOT IN ('PL1048')) 
      AND (CUSTOMER != 'PL1048' OR CUSTOMER IS NULL)  -- REGLA DE NEGOCIO
    
      --AND biczvv160 NOT IN ('IC') 
    GROUP BY 1,2,3,4,5,6
    ORDER BY 2,1
)

/**************************/
/*********STEP 1**********/
/*************************/

,FINAL_COPA AS (
  SELECT 
  CASE
    WHEN REGEXP_REPLACE(CUSTOMER, '^0+', '') IN ('100067548','156049958','156049959','156191760','156191761') THEN 'PARAGUAY'
    ELSE 'URUGUAY'
  END AS COUNTRY --COUNTRY
  ,CASE
    WHEN CUSTOMER IS NULL THEN Param_customer
    WHEN SKU IN ("76222105759801","76222106144000","76222017596000") AND CUSTOMER IN (Param_customer) THEN '156191761'
    ELSE REGEXP_REPLACE(CUSTOMER, '^0+', '')
  END AS CUSTOMER --,CUSTOMER
  ,FISCPER AS PERIOD_YEAR
  ,CASE
    WHEN SKU IS NULL THEN Param_sku
    ELSE CONCAT('SK',SKU)--REGEXP_REPLACE(SKU, '^0+', 'SK')
  END AS SKU --,SKU
  ,DOC_NUMBER AS SALES_ORDER
  ,CURRENCY_COPA
  ,SUM(COGS_LOGISTIC) COGS_LOGISTIC
  ,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING
  ,(SUM(Trade_Incentives) + SUM(Other_Deductions) + SUM(SALES_ALLOW_RETURNS) + SUM(NPD)) AS  G2N
  ,SUM(Gross_Sales) GROSS_SALES
  --,SUM(Gross_Sales) - (SUM(Trade_Incentives) + SUM(Other_Deductions) + SUM(SALES_ALLOW_RETURNS) + SUM(NPD))*-1 AS NET_REVENUE 
  --,(SUM(Gross_Sales) - (SUM(Trade_Incentives) + SUM(Other_Deductions) + SUM(SALES_ALLOW_RETURNS) + SUM(NPD))*-1) - (SUM(COGS_MANUFACTURING)  + SUM(COGS_LOGISTIC) ) AS GROSS_PROFIT
  ,SUM(Volume) VOLUME_NET
  FROM TOTAL_LA_COPA_DATA_BY_SKU
  --
  --(CUSTOMER != 'PL1048' OR CUSTOMER IS NULL) 
 -- (CUSTOMER NOT IN ('PL1048')) 
  GROUP BY 1,2,3,4,5,6
 -- HAVING COGS_LOGISTIC != 0 OR COGS_MANUFACTURING != 0 OR G2N !=0 OR NET_REVENUE!=0 OR VOLUME_NET !=0 OR GROSS_SALES !=0
)



/*************************************/
/*********JERARQUIA PRODUCTOS**********/
/*************************************/

,PRODUCT_HIERARCHY AS (
  SELECT
    a.sku AS SKU
    ,a.categoryid_desc AS CATEGORY
    ,b.brp_desc AS BRAND
    ,a.bws_desc AS BSP
    ,a.ff_desc AS FF
  FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_prod_hier_sku_amer` a
  LEFT JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_product_brand_hier_amer`  b ON a.bws = b.brand_segment
)

/*************************************/
/*********JERARQUIA CLIENTES**********/
/*************************************/

,CUSTOMER_HIERARCHY AS (
    SELECT
  FL.sales_org_cd AS BU
  ,CASE
      WHEN sales_cust_hier_l2_ds = 'MODERN TRADE' THEN 'MT_UY'
      WHEN sales_cust_hier_l2_ds = 'TRADITIONAL TRADE' THEN 'TD_UY'
      WHEN sales_cust_hier_l2_ds = 'TT Export' THEN 'TD_PARAGUAY'
      WHEN sales_cust_hier_l2_ds = 'EXP-EXPORTS-1' THEN 'TD_PARAGUAY'
      ELSE sales_cust_hier_l2_ds
    END AS CHANNEL
  ,sales_cust_hier_l5_ds AS SUBCHANNEL
  ,DC.customer_id AS CUSTOMER
  ,DC.customer_name1_nm AS CUSTOMER_NAME
  FROM `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_dim__customer_salesarea_flattened_hierarchies_test` FL
  INNER JOIN `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_dim__customer_salesarea_test` DCSA
    ON FL.dim_customer_id_salesarea = DCSA.dim_customer_id_salesarea
  INNER JOIN `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_dim__customer_test` DC
    ON DCSA.customer_id = DC.customer_id
  WHERE
    FL.hierarchy_type_cd = 'A'
    AND FL.sales_org_cd = Company_code  --'UY02'
    AND sales_cust_hier_l2_ds IN ('MODERN TRADE', 'TRADITIONAL TRADE','TT Export','EXP-EXPORTS-1')
  ORDER BY 2 ASC
)


/**************************************/
/*********COPA_PRODUCT_JOINED**********/
/**************************************/

,COPA_PRODUCT_JOINED AS (
  SELECT 
    A.COUNTRY
    ,A.CUSTOMER
    ,A.PERIOD_YEAR
    ,A.SKU
    ,A.CURRENCY_COPA
    ,A.SALES_ORDER
    ,B.CATEGORY
    ,B.BSP
    ,B.BRAND
    ,B.FF
    ,SUM(A.COGS_LOGISTIC) COGS_LOGISTIC
    ,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING
    ,SUM(A.G2N) G2N
    ,SUM(A.GROSS_SALES) GROSS_SALES
    ,SUM(A.VOLUME_NET)VOLUME_NET
    --,SUM(A.NET_REVENUE) NET_REVENUE --N
    --,SUM(A.GROSS_PROFIT) GROSS_PROFIT --N
  FROM FINAL_COPA A 
  INNER JOIN PRODUCT_HIERARCHY B
  ON A.SKU=B.SKU
  GROUP BY 1,2,3,4,5,6,7,8,9,10
)

/**************************************/
/*********COPA_PRODUCT_LEFT************/
/**************************************/
,COPA_PRODUCT_LEFT AS (
  SELECT 
       A.PERIOD_YEAR PERIOD_YEAR 
      ,A.SKU SKU
      ,SUM(A.COGS_LOGISTIC) COGS_LOGISTIC
      ,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING
      ,SUM(A.G2N) G2N
      ,SUM(A.GROSS_SALES) GROSS_SALES
      ,SUM(VOLUME_NET)VOLUME_NET
      --,SUM(NET_REVENUE) NET_REVENUE --N
      --,SUM(GROSS_PROFIT) GROSS_PROFIT --N
  FROM FINAL_COPA A 
  LEFT JOIN PRODUCT_HIERARCHY B
  ON A.SKU=B.SKU
  WHERE B.SKU IS NULL
  GROUP BY 1,2
)

/************************************************/
/*********COPA_PRODUCT_CUSTOMER_JOINED***********/
/***********************************************/
,COPA_PRODUCT_CUSTOMER_JOINED AS (
  SELECT
       DISTINCT A.SKU 
      ,A.COUNTRY
      ,A.CUSTOMER
      ,B.CUSTOMER_NAME
      ,B.CHANNEL
      ,B.SUBCHANNEL
      ,A.PERIOD_YEAR
      ,A.SALES_ORDER
      ,A.CURRENCY_COPA
      ,A.CATEGORY
      ,A.BSP
      ,A.BRAND
      ,A.FF
      ,MAX(A.COGS_LOGISTIC) COGS_LOGISTIC
      ,MAX(A.COGS_MANUFACTURING) COGS_MANUFACTURING
      ,MAX(A.G2N) G2N
      ,MAX(A.GROSS_SALES) GROSS_SALES
      ,MAX(A.VOLUME_NET)VOLUME_NET 
      --,MAX(A.NET_REVENUE) NET_REVENUE --N
      --,MAX(A.GROSS_PROFIT) GROSS_PROFIT --N
  FROM COPA_PRODUCT_JOINED A 
  LEFT JOIN (SELECT DISTINCT CUSTOMER,CUSTOMER_NAME,CHANNEL,SUBCHANNEL FROM CUSTOMER_HIERARCHY ) B
  ON 
  A.CUSTOMER=B.CUSTOMER --AND B.CUSTOMER IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)



/*********************************************/
/*********COPA_PRODUCT_CUSTOMER_LEFT**********/
/********************************************/
,COPA_PRODUCT_CUSTOMER_LEFT AS (
  SELECT
       A.PERIOD_YEAR PERIOD_YEAR 
      ,A.SKU SKU
      ,SUM(A.COGS_LOGISTIC) COGS_LOGISTIC
      ,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING
      ,SUM(A.G2N) G2N
      ,SUM(A.GROSS_SALES) GROSS_SALES
      ,SUM(VOLUME_NET)VOLUME_NET 
      --,SUM(NET_REVENUE) NET_REVENUE --N
      --,SUM(GROSS_PROFIT) GROSS_PROFIT --N
  FROM COPA_PRODUCT_JOINED A 
  LEFT JOIN CUSTOMER_HIERARCHY B
  ON 
  A.CUSTOMER=B.CUSTOMER
  WHERE B.CUSTOMER IS NULL
  GROUP BY 1,2
)


/*********************************/
/*********UNION ALL LEFT**********/
/********************************/
--SUMA DE COGS_MANUFACTURING POR PERIODO DE "COPA_PRODUCT_LEFT" Y "COPA_PRODUCT_CUSTOMER_LEFT"
,UNION_ALL_LEFT_COGS AS (
  SELECT PERIOD_YEAR, SUM(COGS_MANUFACTURING) COGS_MANUFACTURING_DIS FROM 
  (SELECT * FROM COPA_PRODUCT_LEFT
  UNION ALL
  SELECT * FROM COPA_PRODUCT_CUSTOMER_LEFT)
  GROUP BY 1
)


--AGREGAR LA COLUMNA COGS_MANUFACTURING(POR PERIODO) A  COPA_PRODUCT_CUSTOMER_JOINED
,INNER_JOIN_3 AS (
    SELECT A.*, B.COGS_MANUFACTURING_DIS COGS_MANUFACTURING_DIS  
    FROM COPA_PRODUCT_CUSTOMER_JOINED A 
    INNER JOIN UNION_ALL_LEFT_COGS B
    ON A.PERIOD_YEAR=B.PERIOD_YEAR
)


--SUMA DE VOLUME POR PERIODO "INNER_JOIN_3"
,SUMMARIZE_2 AS (
  SELECT PERIOD_YEAR, SUM(VOLUME_NET) VOLUME_NET_YEAR FROM INNER_JOIN_3
  GROUP BY 1
)

--AGREGAR COLUMNA DE VOLUME A LA VISTA
,INNER_JOIN_4 AS (
  SELECT A.*, B.VOLUME_NET_YEAR VOLUME_NET_YEAR
  FROM INNER_JOIN_3 A INNER JOIN SUMMARIZE_2 B
  ON A.PERIOD_YEAR=B.PERIOD_YEAR 
)

 --GENERAR LA VISTA OUTPUT_2
,OUTPUT_2 AS (
  SELECT 
  COUNTRY
  ,CUSTOMER
  ,CUSTOMER_NAME
  ,CHANNEL
  ,SUBCHANNEL
  ,PERIOD_YEAR
  ,SKU
  ,SALES_ORDER
  ,CURRENCY_COPA
  ,CATEGORY
  ,BSP
  ,BRAND
  ,FF
  ,COGS_LOGISTIC
  ,G2N
  ,GROSS_SALES
  ,VOLUME_NET

  ,CASE 
  WHEN VOLUME_NET != 0 THEN (COGS_MANUFACTURING_DIS * SAFE_DIVIDE(VOLUME_NET,VOLUME_NET_YEAR)) + COGS_MANUFACTURING
  ELSE COGS_MANUFACTURING
  END COGS_MANUFACTURING 
  FROM INNER_JOIN_4 A
)


--CREAR COLUMNA FLAG DE LA VISTA OUTPUT_2 CUANDO VOLUME PARTICIONADO POR (SKU,COUNTRY,PERIOD_YEAR,SALES_ORDER) >= POS DE LO CONTRARIO NEG
,STEP3 AS (
  select  A.*, IF (SUM(VOLUME_NET) OVER (PARTITION BY SKU,COUNTRY,PERIOD_YEAR,SALES_ORDER) >= 0 ,'VOL_POS','VOL_NEG') FLAG from OUTPUT_2 A 
)

--PARA LOS VALORES QUE FUERON POSITIVOS
,STEP3_DIST_COGS_MAN AS(
  SELECT *
  ,SUM(VOLUME_NET) OVER (PARTITION BY SKU,COUNTRY,PERIOD_YEAR) VOLUME_SKU
  ,SUM(VOLUME_NET) OVER (PARTITION BY FF,COUNTRY,PERIOD_YEAR) VOLUME_FF
  ,SUM(VOLUME_NET) OVER (PARTITION BY BSP,COUNTRY,PERIOD_YEAR) VOLUME_BSP
  ,SUM(VOLUME_NET) OVER (PARTITION BY CATEGORY,COUNTRY,PERIOD_YEAR) VOLUME_CATEGORY
  ,SUM(VOLUME_NET) OVER (PARTITION BY COUNTRY,PERIOD_YEAR) VOLUME_COUTRY_PERIOD

  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY SKU,COUNTRY,PERIOD_YEAR) CM_SKU
  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY FF,COUNTRY,PERIOD_YEAR) CM_FF
  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY BSP,COUNTRY,PERIOD_YEAR) CM_BSP
  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY CATEGORY,COUNTRY,PERIOD_YEAR) CM_CATEGORY
  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY COUNTRY,PERIOD_YEAR) CM_COUNTRY_PERIOD
  FROM  STEP3 WHERE FLAG='VOL_POS'
)


, STEP3_DIST_COGS_MAN_1 AS (
  SELECT *  
  ,SUM(CM_ALOC_BY_ROW_TO_FF) OVER (PARTITION BY FF,COUNTRY,PERIOD_YEAR) CV_ALOC_FF
  ,SUM(CV_ALOC_BY_ROW_TO_BSP) OVER (PARTITION BY BSP,COUNTRY,PERIOD_YEAR) CV_ALOC_BSP
  ,SUM(CV_ALOC_BY_ROW_TO_CAT) OVER (PARTITION BY CATEGORY,COUNTRY,PERIOD_YEAR) CV_ALOC_CAT
  ,SUM(CV_ALOC_BY_ROW_TO_COUNTRY) OVER (PARTITION BY COUNTRY,PERIOD_YEAR) CV_ALOC_COUNTRY_PER
  FROM 
  (
  SELECT * 
  ,VOLUME_NET * SAFE_DIVIDE(CM_SKU,VOLUME_SKU) AS CM_SKU_LEVEL
  ,CASE WHEN VOLUME_SKU=0 AND VOLUME_FF!=0 THEN COGS_MANUFACTURING ELSE 0 END CM_ALOC_BY_ROW_TO_FF
  ,CASE WHEN VOLUME_FF=0 AND VOLUME_BSP!=0 THEN COGS_MANUFACTURING ELSE 0 END CV_ALOC_BY_ROW_TO_BSP
  ,CASE WHEN VOLUME_BSP=0 AND VOLUME_CATEGORY!=0 THEN COGS_MANUFACTURING ELSE 0 END CV_ALOC_BY_ROW_TO_CAT
  ,CASE WHEN VOLUME_CATEGORY=0 AND VOLUME_COUTRY_PERIOD!=0 THEN COGS_MANUFACTURING ELSE 0 END CV_ALOC_BY_ROW_TO_COUNTRY
  FROM STEP3_DIST_COGS_MAN
  )
)-- REVISAR FORMULA DE CONDICIONALES VOL_SKU = 0, VOL_FF = 0


, STEP3_DIST_COGS_MAN_OUTPUT AS (
  SELECT *
  ,VOLUME_NET* SAFE_DIVIDE(CV_ALOC_FF,VOLUME_FF)  AS CM_FF_LEVEL
  ,VOLUME_NET*SAFE_DIVIDE(CV_ALOC_BSP,VOLUME_BSP)  AS CM_BSP_LEVEL
  ,VOLUME_NET*SAFE_DIVIDE(CV_ALOC_CAT,VOLUME_CATEGORY)  AS CV_CAT_LEVEL
  ,VOLUME_NET*SAFE_DIVIDE(CV_ALOC_COUNTRY_PER,VOLUME_COUTRY_PERIOD)  AS CV_COUNTRY_PER_LEVEL
  ,(CM_SKU_LEVEL)+(VOLUME_NET*SAFE_DIVIDE(CV_ALOC_FF,VOLUME_FF)) + (VOLUME_NET*SAFE_DIVIDE(CV_ALOC_BSP,VOLUME_BSP) ) + (VOLUME_NET*SAFE_DIVIDE(CV_ALOC_CAT,VOLUME_CATEGORY)) + (VOLUME_NET*SAFE_DIVIDE(CV_ALOC_COUNTRY_PER,VOLUME_COUTRY_PERIOD) ) AS CM_ADJUSTED
  FROM STEP3_DIST_COGS_MAN_1
)


--PARA LOS VALORES QUE FUERON NEGATIVOS
,STEP3_SALES_ORDER_COGS_MAN AS(
  SELECT * 
  ,SUM(VOLUME_NET) OVER (PARTITION BY SKU,COUNTRY,PERIOD_YEAR) VOLUME_SKU
  ,SUM(COGS_MANUFACTURING) OVER (PARTITION BY SKU,COUNTRY,PERIOD_YEAR) CM_SKU
  FROM STEP3 
  WHERE FLAG='VOL_NEG'
)

,STEP3_SALES_ORDER_COGS_MAN_OUTPUT AS (
  SELECT *
  ,CM_SKU* SAFE_DIVIDE(VOLUME_NET,VOLUME_SKU)  AS CM_ADJUSTED
  FROM STEP3_SALES_ORDER_COGS_MAN
)

--GENERAR OUTPUT_3 CON UNION ALL
,OUTPUT_3 AS (
  SELECT CATEGORY,CUSTOMER,CUSTOMER_NAME,BSP,BRAND,FF,SKU,COUNTRY,CURRENCY_COPA,PERIOD_YEAR,CHANNEL,SUBCHANNEL,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING,SUM(COGS_LOGISTIC) COGS_LOGISTIC,SUM(G2N)*-1 G2N,SUM(GROSS_SALES) GROSS_SALES,SUM(VOLUME_NET) VOLUME_NET,COALESCE(SUM(CM_ADJUSTED),0) CM_ADJUSTED 
  FROM STEP3_DIST_COGS_MAN_OUTPUT
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 HAVING GROSS_SALES !=0 OR VOLUME_NET !=0 OR G2N!=0 OR COGS_LOGISTIC!=0 OR CM_ADJUSTED!=0
  UNION ALL
  SELECT CATEGORY,CUSTOMER,CUSTOMER_NAME,BSP,BRAND,FF,SKU,COUNTRY,CURRENCY_COPA,PERIOD_YEAR,CHANNEL,SUBCHANNEL,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING,SUM(COGS_LOGISTIC) COGS_LOGISTIC,SUM(G2N)*-1 G2N,SUM(GROSS_SALES) GROSS_SALES,SUM(VOLUME_NET) VOLUME_NET,COALESCE(SUM(CM_ADJUSTED),0) CM_ADJUSTED 
  FROM STEP3_SALES_ORDER_COGS_MAN_OUTPUT
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 HAVING GROSS_SALES !=0 OR VOLUME_NET !=0 OR G2N!=0 OR COGS_LOGISTIC!=0 OR CM_ADJUSTED!=0
)


,OUPUT_FIT AS (
  SELECT * 
  FROM
  (
    SELECT PERIOD_YEAR
    ,CASE
      WHEN account = "PL211005" THEN "COGS_MANUFACTURING"
      WHEN account = "PL212099" THEN "COGS_LOGISTIC"
      WHEN account = "PL201010" THEN "GROSS_SALES"
      WHEN account = "PL202000" THEN "G2N"
      WHEN account = "PLV959999" THEN "VOLUME"
    END AS ACCOUNT
    ,CASE
      WHEN entity in ("CDBOLFS","BOALLOC","BOLFS") THEN "BOLIVIA"
      WHEN entity in ('ARUFS_Local') THEN "ARGENTINA"
      WHEN entity in ('BRLFS') THEN "BRAZIL"
      WHEN entity in ('SVLFS') THEN "EL SALVADOR"
      WHEN entity in ('GTLFS') THEN "GUATEMALA"
      WHEN entity in ('HNLFS') THEN "HONDURAS"
      WHEN entity in ('MXLFS') THEN "MEXICO"
      WHEN entity in ('NILFS') THEN "NICARAGUA"
      WHEN entity in ('PALFS') THEN "PANAMA"
      WHEN entity in ('DOLFS') THEN "REPUBLICA DOMINICANA"
      WHEN entity in ('UYLFS') THEN "URUGUAY"
      WHEN entity = "CLLFS" THEN "CHILE"
      WHEN entity in ("COALLOC","CDCOLCOM","CDCOLOM2_ELIM","CDCOLSC","COLFS","COLFSLAN","COT2_ELIM") THEN "COLOMBIA"
      WHEN entity = "ECLFS" THEN "ECUADOR"
      WHEN entity = "PELFS" THEN "PERU"
      WHEN entity in ("BRLFSKFX","CAMFSUSX","CDWIUFS","CRBUSDEXP","LAEX2_ELIM","LAEXALLOC") THEN "AMEX"
      WHEN entity = "PRUFSKFT" THEN "PUERTO RICO"
      WHEN entity in ("CRLFS", "MBSCRLFS") THEN "COSTA RICA"
    END AS COUNTRY
  ,SKU,
  VALUE
  FROM
  (
    SELECT  
      FD.entity,
      CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date)) PERIOD_YEAR
      ,FA.level_08 account
      ,REPLACE(FD.SKU, 'SKU_SK', 'SK') SKU , ROUND(SUM(FD.value),2) value
    FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.fit_Data` FD
    INNER JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_account_hier_amer`  FA ON FD.account_code = FA.account_code
    INNER JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_entity_hier_amer` FE ON FD.entity = FE.entity
    WHERE 
      FE.entity = Fit_entity 
      AND FD.Currency = "LCL"
      AND FA.level_08 IN ("PL211005" ,"PL212099")
      AND  FD.Date  >  Periodo_ini_fit
    GROUP BY
      FD.entity
      ,CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date))
      ,FA.level_08
      ,REPLACE(FD.SKU, 'SKU_SK', 'SK')
    UNION ALL
    SELECT  
      FD.entity,
      CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date))
      ,FA.level_07,REPLACE(FD.SKU, 'SKU_SK', 'SK') SKU
      ,ROUND(SUM(FD.value),2)
    FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.fit_Data` FD
    INNER JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_account_hier_amer` FA ON FD.account_code = FA.account_code
    INNER JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_entity_hier_amer`  FE ON FD.entity = FE.entity
    WHERE 
      FE.entity = Fit_entity 
      AND FD.Currency = "LCL"
      AND FA.level_07 IN ("PL202000","PL201010")
      AND  FD.Date  > Periodo_ini_fit
    GROUP BY
      FD.entity
      ,CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date))
      ,FA.level_07
      ,REPLACE(FD.SKU, 'SKU_SK', 'SK')
    UNION ALL
    SELECT  
      FD.entity
      ,CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date)) PERIOD_YEAR
      ,FD.account_code ACCOUNT
      ,REPLACE(FD.SKU, 'SKU_SK', 'SK') SKU , ROUND(SUM(FD.value),2) value
    FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.fit_Data` FD
    WHERE 
      FD.entity = Fit_entity 
      AND FD.Currency = "LCL"
      AND FD.account_code IN ('PLV959999')
      AND  FD.Date  >  Periodo_ini_fit
    GROUP BY 
      FD.entity
      ,CONCAT(FORMAT_DATE('%Y', FD.Date), '0',FORMAT_DATE('%m', FD.Date)) 
      ,FD.account_code
      ,REPLACE(FD.SKU, 'SKU_SK', 'SK')
  )--WHERE PERIOD_YEAR ='2024007'
  ) 
  PIVOT
  (
    SUM(VALUE) FOR ACCOUNT IN ('COGS_MANUFACTURING','COGS_LOGISTIC','GROSS_SALES','G2N','VOLUME')
  )
)


,OUTPUT_FIT_REVENUE AS (
  SELECT 
  SKU
  , PERIOD_YEAR
  ,COUNTRY
  ,COALESCE(SUM(COGS_MANUFACTURING),0)*-1 COGS_MANUFACTURING
  ,SUM(COGS_LOGISTIC)*-1 COGS_LOGISTIC
  ,SUM(GROSS_SALES) GROSS_SALES 
  ,SUM(G2N) *-1 G2N
  ,SUM(VOLUME) VOLUME
  ,COALESCE(SUM(GROSS_SALES),0) - COALESCE(SUM(G2N),0)*-1 AS NET_REVENUE  
  ,(COALESCE(SUM(GROSS_SALES),0) - COALESCE(SUM(G2N),0)*-1) - (COALESCE(SUM(COGS_MANUFACTURING),0) + COALESCE(SUM(COGS_LOGISTIC),0))*-1 AS GROSS_PROFIT 
  FROM OUPUT_FIT 
  GROUP BY 1,2,3

)


,OUTPUT_DIF AS (

    SELECT 
  B.PERIOD_YEAR
  ,B.SKU
  ,B.COUNTRY
  ,A.CURRENCY_COPA
  ,A.BSP
  ,A.BRAND
  ,A.CATEGORY
  ,A.FF
  ,COALESCE(SUM(B.COGS_MANUFACTURING),0) - COALESCE(SUM(A.COGS_MANUFACTURING),0)   COGS_MANUFACTURING
  ,COALESCE(SUM(B.COGS_LOGISTIC),0) - COALESCE(SUM(A.COGS_LOGISTIC),0)   COGS_LOGISTIC
  ,COALESCE(SUM(B.GROSS_SALES),0) - COALESCE(SUM(A.GROSS_SALES),0)   GROSS_SALES
  ,COALESCE(SUM(B.G2N),0) - COALESCE(SUM(A.G2N),0)   G2N
  ,COALESCE(SUM(B.VOLUME),0) - COALESCE(SUM(A.VOLUME),0) VOLUME
  FROM
  OUTPUT_FIT_REVENUE B
  LEFT JOIN (SELECT CURRENCY_COPA,PERIOD_YEAR,SKU,BSP,BRAND,CATEGORY,FF,SUM(CM_ADJUSTED) COGS_MANUFACTURING,SUM(COGS_LOGISTIC) COGS_LOGISTIC,SUM(GROSS_SALES)GROSS_SALES,SUM(G2N)G2N,SUM(VOLUME_NET)VOLUME FROM  OUTPUT_3
  GROUP BY 1,2,3,4,5,6,7) A 
  ON B.SKU=A.SKU AND B.PERIOD_YEAR=A.PERIOD_YEAR   GROUP BY 1,2,3,4,5,6,7,8

)


,base_data AS (
    SELECT
        year,
        PARSE_DATE('%Y-%m-%d',  
        CONCAT(CAST(year AS STRING), '-',  
        CASE
            WHEN period = 'Jan' THEN '01'
            WHEN period = 'Feb' THEN '02'
            WHEN period = 'Mar' THEN '03'
            WHEN period = 'Apr' THEN '04'
            WHEN period = 'May' THEN '05'
            WHEN period = 'Jun' THEN '06'
            WHEN period = 'Jul' THEN '07'
            WHEN period = 'Aug' THEN '08'
            WHEN period = 'Sep' THEN '09'
            WHEN period = 'Oct' THEN '10'
            WHEN period = 'Nov' THEN '11'
            WHEN period = 'Dec' THEN '12'
        END, '-01')) AS DATE,
        period,  
        version,  
        currency,  
        scenario,  
        value
    FROM
        `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_curr_exch_rates_amer`  
    WHERE
        account='Avg_Rate'
        AND scenario IN ("Act","AC")
        AND version = "Final"
)

, pivot_data AS (
    SELECT
        year,
        DATE,
        period,
        version,
        currency,
        scenario,
        MAX(CASE WHEN scenario = 'AC' THEN value END) AS AC,
        MAX(CASE WHEN scenario = 'Act' THEN value END) AS Act
    FROM
        base_data
    GROUP BY
        year,
        DATE,
        period,
        version,
        currency,
        scenario
)
 
, py_pivot_data AS (
      SELECT
        year,
        DATE,
        DATE_ADD(DATE, INTERVAL 1 YEAR) AS NewDate,
        period,
        version,
        currency,
        MAX(CASE WHEN scenario = 'AC' THEN value END) AS ACPYRate,
        MAX(CASE WHEN scenario = 'Act' THEN value END) AS ActPYRate
    FROM
        base_data
    GROUP BY
        year,
        DATE,
        NewDate,
        period,
        version,
        currency
)
 
,FINAL_EXCHANGE_RATES_TABLE AS (
  SELECT
    LEFT(SAFE_CAST(a.date AS STRING),4)||"0" || SUBSTRING(SAFE_CAST(a.date AS STRING),6,2) AS fiscper,
    a.version,
    a.currency,
    a.AC AS ACRate, --ACRATE
    a.Act AS ActRate, 
    a.scenario,
    b.ACPYRate, 
    b.ActPYRate --PYRATE
  FROM pivot_data a
  LEFT JOIN  py_pivot_data b
    ON a.date = b.NewDate
      AND a.currency = b.currency
  WHERE a.date between Periodo_ini and Periodo_fin
)

,OUTPUT_UNION_ALL AS (
  SELECT SKU,'-1' CUSTOMER,'-1' CUSTOMER_NAME,COUNTRY, CURRENCY_COPA, BSP,BRAND, CATEGORY, FF,PERIOD_YEAR,'-1' CHANNEL,'-1' SUBCHANNEL,GROSS_SALES,G2N,VOLUME,(GROSS_SALES - G2N) NET_REVENUE,(COALESCE((GROSS_SALES - G2N),0) - COALESCE((COGS_MANUFACTURING+COGS_LOGISTIC),0)) GROSS_PROFIT,COGS_LOGISTIC,COGS_MANUFACTURING 
  FROM OUTPUT_DIF 
  UNION ALL
  SELECT SKU,CUSTOMER,CUSTOMER_NAME,COUNTRY,CURRENCY_COPA,BSP,BRAND,CATEGORY,FF,PERIOD_YEAR,CHANNEL,SUBCHANNEL,GROSS_SALES,G2N,VOLUME_NET VOLUME,(GROSS_SALES - G2N) NET_REVENUE,((GROSS_SALES - G2N) - (CM_ADJUSTED+COGS_LOGISTIC)) GROSS_PROFIT,COGS_LOGISTIC,CM_ADJUSTED COGS_MANUFACTURING 
  FROM OUTPUT_3 
)





,CLUSTERS AS (
  SELECT
  UPPER(country) AS COUNTRY
  ,cluster1
  ,cluster2 
  FROM `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_manual_country_cluster_mapping`
  WHERE fit_entity = Fit_entity
)


,OUTPUT_PIVOT AS (
  SELECT * 
  FROM
  (
    SELECT
    SKU
    ,CUSTOMER
    ,CUSTOMER_NAME
    ,A.COUNTRY
    ,BSP
    ,BRAND
    ,CATEGORY
    ,FF
    ,PERIOD_YEAR
    ,CHANNEL
    ,SUBCHANNEL
    ,CURRENCY_COPA
    ,SUM(GROSS_SALES) GROSS_SALES 
    ,SUM(G2N) G2N 
    ,SUM(NET_REVENUE) AS NET_REVENUE
    ,SUM(GROSS_PROFIT) AS GROSS_PROFIT
    ,SUM(VOLUME) VOLUME
    ,SUM(COGS_LOGISTIC) COGS_LOGISTIC
    ,SUM(COGS_MANUFACTURING) COGS_MANUFACTURING 
    FROM OUTPUT_UNION_ALL A 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  )
  UNPIVOT( AMOUNT FOR ACCOUNT IN (GROSS_SALES,G2N,VOLUME,COGS_LOGISTIC,COGS_MANUFACTURING,GROSS_PROFIT,NET_REVENUE))
)

,OUTPUT_RATE AS (
  SELECT 
  SKU
  ,CUSTOMER
  ,CUSTOMER_NAME
  ,COUNTRY
  ,BSP
  ,BRAND
  ,CATEGORY
  ,FF
  ,PERIOD_YEAR
  ,CHANNEL
  ,SUBCHANNEL
  ,ACCOUNT
  ,SUM(AMOUNT) AMOUNT_LCL
  ,CASE WHEN ACCOUNT!='VOLUME' THEN SUM(AMOUNT) / MAX(ActRate) ELSE SUM(AMOUNT) END AMOUNT_USD
  ,CASE WHEN ACCOUNT!='VOLUME' THEN SUM(AMOUNT) / MAX(ACRate) ELSE SUM(AMOUNT) END Value_AC_Rate
  ,CASE WHEN ACCOUNT!='VOLUME' THEN SUM(AMOUNT) / MAX(ActPYRate) ELSE SUM(AMOUNT) END Value_PY_Rate
  FROM
  OUTPUT_PIVOT A
  LEFT JOIN (SELECT FISCPER,CURRENCY,MAX(ACRate) ACRate,MAX(ActRate) ActRate,MAX(ACPYRATE) ACPYRATE,MAX(ACTPYRATE)ACTPYRATE  FROM FINAL_EXCHANGE_RATES_TABLE
  GROUP BY FISCPER,CURRENCY) B
  ON A.CURRENCY_COPA=B.currency AND A.PERIOD_YEAR=B.fiscper
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

 -- AÑADIR LOS VALORES CONVERTIDOS A LAS TASAS DE AC Y PY
 
,OUTPUT_FINAL AS (
  SELECT 
      SKU
      ,CUSTOMER
      ,CUSTOMER_NAME
      ,BRAND
      ,BSP SUBBRAND
      ,CATEGORY
      ,FF FORMAT_FLAVOR
      ,PERIOD_YEAR
      ,PARSE_DATE('%Y/%m/%d',CONCAT(SUBSTR(PERIOD_YEAR, 1, 4), '/',LPAD(CAST(CAST(SUBSTR(PERIOD_YEAR, 5, 3) AS INT64) AS STRING), 2, '0'), '/', '01')) DATE
      ,CHANNEL
      ,SUBCHANNEL
      ,B.Cluster1 Area
      ,B.Cluster2 Cluster
      ,A.COUNTRY Market
      ,'ACT' SCENARIO
       ,CASE WHEN ACCOUNT='COGS_MANUFACTURING' OR ACCOUNT='COGS_LOGISTIC' THEN 'COGS' 
              WHEN ACCOUNT='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN ACCOUNT='G2N' THEN 'Gross to Net'
              WHEN ACCOUNT='GROSS_SALES' THEN 'Gross Sales'
              WHEN ACCOUNT='NET_REVENUE' THEN 'Net Revenue'
              WHEN ACCOUNT='VOLUME' THEN 'Volume'
        END ACCOUNT_LVL_1
        ,CASE WHEN ACCOUNT='COGS_MANUFACTURING' THEN 'COGS_MANUFACTURING'
              WHEN ACCOUNT='COGS_LOGISTIC' THEN 'COGS_LOGISTIC' 
              WHEN ACCOUNT='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN ACCOUNT='G2N' THEN 'Gross to Net'
              WHEN ACCOUNT='GROSS_SALES' THEN 'Gross Sales'
              WHEN ACCOUNT='NET_REVENUE' THEN 'Net Revenue'
              WHEN ACCOUNT='VOLUME' THEN 'VOLUME'
        END ACCOUNT_LVL_2
        ,CASE WHEN ACCOUNT='COGS_MANUFACTURING' THEN 'COGS_MANUFACTURING'
              WHEN ACCOUNT='COGS_LOGISTIC' THEN 'COGS_LOGISTIC' 
              WHEN ACCOUNT='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN ACCOUNT='G2N' THEN 'Gross to Net'
              WHEN ACCOUNT='GROSS_SALES' THEN 'Gross Sales'
              WHEN ACCOUNT='NET_REVENUE' THEN 'Net Revenue'
              WHEN ACCOUNT='VOLUME' THEN 'Volume'
        END ACCOUNT_LVL_3
      ,SUM(Value_AC_Rate) Value_AC_Rate
      ,SUM(Value_PY_Rate) Value_PY_Rate
      ,SUM(AMOUNT_USD) Value_USD
      ,SUM(AMOUNT_LCL) Value_LCL
  --FROM AGRUPADO A
  FROM OUTPUT_RATE A
  LEFT JOIN CLUSTERS B
  ON A.COUNTRY=B.COUNTRY
  GROUP BY     SKU,CUSTOMER
      ,CUSTOMER_NAME
      ,BSP
      ,BRAND
      ,CATEGORY
      ,FF
      ,PERIOD_YEAR
      ,CHANNEL
      ,SUBCHANNEL
      ,B.Cluster1
      ,B.Cluster2
      ,A.COUNTRY
      ,ACCOUNT
   --   ,SCENARIO
)


,FIT_PLANNING_AC AS (
  SELECT
    scenario,
    Date,
    account,
    entity,
    product,
    currency,
    Channel,
    version,
    SUM(Value_LCL) AS Value_LCL,
    SUM(Value_USD) AS Value_USD
  FROM
    `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.t_FIT_Data_Planning`
  WHERE
    Scenario = "AC"
    AND account NOT IN ("PLC203000_50")
    AND entity IN (Fit_entity)
    AND Date between Periodo_ini and Periodo_fin
  GROUP BY 1,2,3,4,5,6,7,8
)

,FIT_PLANNING AS (
    SELECT 
    SKU
    ,Channel
    ,CATEGORY
    ,FIT_Data.Date
    ,FIT_Data.scenario
    ,FORMAT_FLAVOR
    ,SUBBRAND
    ,BRAND
    ,CLUSTER1 Area
    ,CLUSTER2 Cluster
    ,country_hier_mapping.Country Market
   -- ,b.account
    ,CASE
      WHEN B.account = "PL211005" THEN "COGS_MANUFACTURING"
      WHEN B.account = "PL212099" THEN "COGS_LOGISTIC"
      WHEN B.account = "PL201010" THEN "GROSS_SALES"
      WHEN B.account = "PL202000" THEN "G2N"
      WHEN B.account = "PL213999" THEN "GROSS_PROFIT"
      WHEN B.account = "PLV959999" THEN "VOLUME"
    END AS CUENTA
    ,SUM(Value_LCL) Value_LCL
    ,SUM(Value_USD)Value_USD FROM
    (SELECT
      *,
      CASE WHEN ENTITY='UYLFS' THEN 'URUGUAY' END AS country
      FROM  FIT_PLANNING_AC
     ) FIT_Data -- FIT DATA 
        LEFT JOIN 
    (
      SELECT
        a.categoryid_desc AS CATEGORY
        ,b.brp_desc AS BRAND
        ,a.bws bws
        ,a.bws_desc AS SUBBRAND
        ,ANY_VALUE(a.ff_desc) AS FORMAT_FLAVOR
        ,ANY_VALUE(a.sku) AS SKU
      FROM `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_prod_hier_sku_amer` a
      LEFT JOIN `prd-amer-analyt-actuals-svc-0a.amer_p_la_fin_data_hub.v_fit_product_brand_hier_amer`  b ON a.bws = b.brand_segment
      GROUP BY 1,2,3,4
    ) prod_hier --JERARQUIA DE PRODUCTOS
    ON FIT_Data.product =prod_hier.bws
    INNER JOIN
    (
      SELECT DISTINCT account_code,level_05 account,level_05_description descripcion
      FROM `prd-amer-analyt-datal-svc-88.amer_h_fit_mst.v_fit_account_hier_amer` 
      WHERE  level_05 IN ("PLV959999","PL213999")
      GROUP BY 1,2,3
      UNION ALL
      SELECT DISTINCT account_code,level_08 account,level_08_description descripcion
      FROM `prd-amer-analyt-datal-svc-88.amer_h_fit_mst.v_fit_account_hier_amer` 
      WHERE  level_08 IN ("PL211005" ,"PL212099")
      GROUP BY 1,2,3
      UNION ALL
      SELECT DISTINCT account_code,level_07 account,level_07_description descripcion
      FROM `prd-amer-analyt-datal-svc-88.amer_h_fit_mst.v_fit_account_hier_amer` 
      WHERE  level_07 IN ("PL202000","PL201010") 
      GROUP BY 1,2,3
    ) b -- v_fit_account_hier_amer
    ON FIT_Data.account = b.account_code
    LEFT JOIN (
      SELECT
      UPPER(country) AS COUNTRY
      ,cluster1
      ,cluster2 
      FROM `dev-amer-analyt-actuals-svc-7a.amer_p_la_fin_data_hub.v_manual_country_cluster_mapping`) country_hier_mapping
    ON FIT_Data.country=country_hier_mapping.country 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    order by 1,2,3,4,5
)

,FIT_PLANNING_REVENUE AS (
  SELECT  
      SKU
      ,Channel
      ,CATEGORY
      ,DATE
      ,SCENARIO
      ,FORMAT_FLAVOR
      ,SUBBRAND
      ,BRAND
      ,Area
      ,Cluster
      ,Market
   --   ,'' account
      ,CUENTA
      ,SUM(Value_USD) Value_USD  
      ,SUM(Value_LCL) Value_LCL
  FROM
  (
    SELECT 
      SKU
      ,Channel
      ,CATEGORY
      ,DATE
      ,SCENARIO
      ,FORMAT_FLAVOR
      ,SUBBRAND
      ,BRAND
      ,Area
      ,Cluster
      ,Market
     --,account
      ,'NET_REVENUE' CUENTA
      ,0 Value_AC_Rate
      ,0 Value_PY_Rate
      ,0 Value_USD
      ,SUM(CASE WHEN CUENTA = 'GROSS_SALES' THEN VALUE_LCL ELSE 0 END) - SUM(CASE WHEN CUENTA = 'G2N' THEN VALUE_LCL ELSE 0 END) AS Value_LCL  
    FROM FIT_PLANNING A
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    UNION ALL
    SELECT
      SKU
      ,Channel
      ,CATEGORY
      ,DATE
      ,SCENARIO
      ,FORMAT_FLAVOR
      ,SUBBRAND
      ,BRAND
      ,Area
      ,Cluster
      ,Market
     -- ,account
      ,'NET_REVENUE' CUENTA
      ,0 Value_AC_Rate
      ,0 Value_PY_Rate
      ,SUM(CASE WHEN CUENTA = 'GROSS_SALES' THEN VALUE_USD ELSE 0 END) - SUM(CASE WHEN CUENTA = 'G2N' THEN VALUE_USD ELSE 0 END) AS Value_USD  
      ,0 Value_LCL  
    FROM FIT_PLANNING A
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  )
  GROUP BY SKU,Channel,CATEGORY,DATE,SCENARIO,FORMAT_FLAVOR,SUBBRAND,BRAND,Area,Cluster,Market,CUENTA
)


,UNION_PLANING AS (
  SELECT * FROM
  (SELECT * FROM FIT_PLANNING
  UNION ALL
  SELECT * FROM FIT_PLANNING_REVENUE
  ORDER BY 1,2,3,4,5,6,7,8,9,10,11,13)
)

,PLANING_FINAL AS(
  SELECT 
    SKU
    ,'-1' CUSTOMER
    ,'-1' CUSTOMER_NAME
    ,BRAND
    ,SUBBRAND
    ,CATEGORY
    ,FORMAT_FLAVOR
    ,CONCAT(FORMAT_DATE('%Y', Date), '0',FORMAT_DATE('%m', Date))  PERIOD_YEAR
    ,DATE
    ,'-1' CHANNEL
    ,'-1' SUBCHANNEL
    ,Area
    ,Cluster
    ,Market
    ,SCENARIO
        ,CASE WHEN CUENTA='COGS_MANUFACTURING' OR CUENTA='COGS_LOGISTIC' THEN 'COGS' 
              WHEN CUENTA='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN CUENTA='G2N' THEN 'Gross to Net'
              WHEN CUENTA='GROSS_SALES' THEN 'Gross Sales'
              WHEN CUENTA='NET_REVENUE' THEN 'Net Revenue'
              WHEN CUENTA='VOLUME' THEN 'Volume'
        END ACCOUNT_LVL_1
        ,CASE WHEN CUENTA='COGS_MANUFACTURING' THEN 'COGS_MANUFACTURING'
              WHEN CUENTA='COGS_LOGISTIC' THEN 'COGS_LOGISTIC' 
              WHEN CUENTA='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN CUENTA='G2N' THEN 'Gross to Net'
              WHEN CUENTA='GROSS_SALES' THEN 'Gross Sales'
              WHEN CUENTA='NET_REVENUE' THEN 'Net Revenue'
              WHEN CUENTA='VOLUME' THEN 'VOLUME'
        END ACCOUNT_LVL_2
        ,CASE WHEN CUENTA='COGS_MANUFACTURING' THEN 'COGS_MANUFACTURING'
              WHEN CUENTA='COGS_LOGISTIC' THEN 'COGS_LOGISTIC' 
              WHEN CUENTA='GROSS_PROFIT' THEN 'Gross Profit'
              WHEN CUENTA='G2N' THEN 'Gross to Net'
              WHEN CUENTA='GROSS_SALES' THEN 'Gross Sales'
              WHEN CUENTA='NET_REVENUE' THEN 'Net Revenue'
              WHEN CUENTA='VOLUME' THEN 'Volume'
        END ACCOUNT_LVL_3
        ,0 Value_AC_Rate
        ,0 Value_PY_Rate
        ,Value_USD
        ,Value_LCL
  FROM UNION_PLANING --WHERE SKU  NOT LIKE '%D_%'
)


,UNION_PLANNING_OUT_FINAL AS (
  SELECT * FROM
  (  SELECT * FROM OUTPUT_FINAL
    UNION ALL
    SELECT * FROM PLANING_FINAL)
  ORDER BY 1,8,16,17,18
)


SELECT * FROM UNION_PLANNING_OUT_FINAL
)
