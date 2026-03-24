------------------------------------------------------------------SQL PRIME----------------------------------------------------------------------------

-------------------------------------------------------------------------EMPLOYER-------------------------------------------------------------------------------------
--Tables--
employer.empr_dtl_h and employer.empr_dtl

--Basic Employer detail search--
SELECT * FROM employer.empr_dtl WHERE "EMPR_ACCT_ID" = 200000793;

SELECT * FROM employer.empr_dtl WHERE "RGST_DT" BETWEEN '2025-09-30' AND '2025-12-31'
ORDER BY "EMPR_ACCT_ID"
limit 500

--want an employer with FEIN for a certain year?--
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e
LEFT JOIN wagedetail.wage_dtl_tran w
ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"
AND "RPT_YR" = '2023'
WHERE w."EMPR_ACCT_ID" IS NULL limit 50

--Search employers registered--Tells you their first wage date(FIRST_WAGE_DT)
SELECT * FROM employer.rgst_empr LIMIT 10
--Can search by ENTITY_NA, DBA_NA, ENTITY_TYPE_CD(CCOR,OTHER,AGNT,CFAC,TPA),BUSINESS_TYPE_CD(RGEM,AGRI,FUTA,EXTR)

--501c3 search--
SELECT * FROM employer.empr_dtl 
WHERE "SRVC_BEGIN_DT" = '2023-01-01'
AND "CONTB_STATUS" = 'Y'
AND "IRS_501C3_IN" = 'Y'
AND "STATUS_CD" = 'ACTV'

--Employer search by reporting method--
SELECT * FROM employer.empr_dtl
 WHERE "REPORT_METHOD_CD" = 'REIM' LIMIT 300 --change to 'CONT'for contributory--
--Another example--
SELECT * FROM employer.empr_dtl WHERE "REPORT_METHOD_CD" > 'CONT' LIMIT 5

--Employer search by Liability Date--
Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01'
--OR--
Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01' --no TPA and empr inactive--
AND 'AGENT_IN' = 'N'
AND 'STATUS_CD' = 'INAC'

--Employer Search by Entity Type--
SELECT * FROM employer.empr_dtl
WHERE "ENTITY_TYPE_CD" = 'CCOR'
AND "STATUS_CD" = 'ACTV'
limit 500

--Employer Contact Info F/L name, Title, ph#, etc--
SELECT * FROM employer.empr_cntct LIMIT 5

--Inactive and Active Employere--
SELECT * FROM employer.empr_dtl
WHERE "STATUS_CD" = 'inac'
limit 5;

--Find Legacy Employers--
SELECT * FROM employer.legacy_employer_xref
 WHERE "empr_acct_id" = '100001088'
 --can also order by--
SELECT * FROM employer.legacy_employer_xref
 ORDER BY legacy_employer_xref DESC LIMIT 100

--Employer Keins that begin with 1 only--
SELECT * FROM employer.empr_dtl 
WHERE "STATUS_CD" = 'ACTV'
	AND "EMPR_ACCT_ID" ::text LIKE '1%'
LIMIT 300

--Converted Employers--
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e
LEFT JOIN wagedetail.wage_dtl_tran w
ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"
AND "RPT_QTR" = '1'
AND "RPT_YR" = '2024'
WHERE w."EMPR_ACCT_ID" IS NULL
and e."STATUS_CD" = 'ACTV'
Limit 1000

--Experience Transfer--
SELECT * FROM employer.expr_trnsfr_hdr LIMIT 1000

--Deeper dive for is converted or not--
--Check if SSN was added before or after conversion--
SELECT 
"SSN", 
"EMPR_ACCT_ID", 
"RPT_YR", 
"RPT_QTR",
"IS_CONV", -- This tells you if it was converted
"CREATE_DT", -- This tells you EXACTLY when it hit the DB
"WAGES_AM"
FROM 
wagedetail.wage_unit_dtl
WHERE 
"EMPR_ACCT_ID" = '102467567' -- Replace with your KEIN
ORDER BY 
"CREATE_DT" DESC;

--Successor and Predessesor--
SELECT * from employer.expr_trnsfr_factors LIMIT 3

--Find out how many SSNS (Employees) an Employer has--
SELECT 
"EMPR_ACCT_ID", 
"EMPR_UNIT_ID", 
COUNT(DISTINCT "SSN") AS "SSN_COUNT"
FROM 
wagedetail.wage_unit_dtl
WHERE
"EMPR_ACCT_ID" = '102467567'
GROUP BY 
"EMPR_ACCT_ID", 
"EMPR_UNIT_ID"
ORDER BY 
"EMPR_ACCT_ID" ASC, 
"EMPR_UNIT_ID" ASC;

--Predessesor and Succesor Employers search--
SELECT * FROM employer.expr_trnsfr_dtl
ORDER BY "SUCC_ID" ASC, "SUCC_SEQ_NU" ASC LIMIT 100

--Predessesor KEIN Search--
SELECT * FROM employer.expr_trnsfr_dtl
WHERE "PRED_EMPR_ACCT_ID" = '101139544'

--Termed Employers(CESD)--
SELECT * FROM EMPLOYER.EMPR_SUSPEND where "EMPR_ACCT_ID" = '200000863';

--Find employers eligible for manual termination--
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e

--Terminated Employer in process--
Select * FROM employer.terminate_empr_staging
WHERE "is_processed" = 'Y';

--Employer Correspondence Preference-- 
SELECT * FROM employer.empr_prfrnc --codes are PAPR and ELEC--
WHERE "BENEFIT_CORRES_TYPE_CD" ='elec' LIMIT 1000

--Employer Language Preference--
SELECT * FROM employer.address WHERE "LANGUAGE_PREFERENCE" = 'sp-us'
SELECT * FROM employer.rgst_addr WHERE "LANGUAGE_PREFERENCE" > 'en'

--Employers with Multiple Reporting Units--
WITH counted AS (
 SELECT
    A.*,
    B.*,
    COUNT(*) OVER (PARTITION BY A."EMPR_ACCT_ID", B."EMPR_UNIT_ID") AS unit_count
  FROM
    employer.empr_dtl A
    JOIN wagedetail.wage_unit_dtl B
      ON A."EMPR_ACCT_ID" = B."EMPR_ACCT_ID"
  WHERE
    A."STATUS_CD" = 'ACTV'
    AND B."RPT_YR" = 2025 
    AND B."RPT_QTR" = 1
    AND B."EMPR_UNIT_ID" != 0
)
SELECT *
FROM counted
WHERE unit_count > 1
ORDER BY unit_count DESC
LIMIT 10;
--OR--
select A."EMPR_ACCT_ID", b."EMPR_UNIT_ID", COUNT(b.*) from employer.empr_dtl A JOIN wagedetail.wage_unit_dtl b 
on A."EMPR_ACCT_ID" = b."EMPR_ACCT_ID"
where A."STATUS_CD" = 'ACTV'
AND b."RPT_YR" = 2025 
AND B."RPT_QTR" = 1
AND b."EMPR_UNIT_ID" != 0
GROUP BY A."EMPR_ACCT_ID", b."EMPR_UNIT_ID"
HAVING COUNT(b.*) > 1 
ORDER BY 3 DESC
LIMIT 10 ;

--Employers with Benefit Charges--
SELECT * FROM wagedetail.chrg_dtl
ORDER BY "CHRG_DTL_ID" ASC LIMIT 1000



----------------------------------------------------------------------BANKRUPTCY-----------------------------------------------------------------------
--Bankruptcy Base search--
SELECT * FROM employer.bankruptcy_dtl;

--Employers with Bankruptcies--
SELECT * FROM employer.empr_dtl 
WHERE "BNKRPCY_STATE_CD" = 'KY'
AND "STATUS_CD" = 'ACTV';

--Find employers who are delinquent and in bankruptcy--
SELECT e.*
     , d.*
FROM employer.empr_dtl e
JOIN wagedetail.delinquent_emp_staging d
  ON e."EMPR_ACCT_ID" = d."EMPR_ACCT_ID"
WHERE e."BNKRPCY_STATE_CD" = 'KY'
  AND e."STATUS_CD" IN ('ACTV', 'RVSD', 'DFLT', 'CLSD', 'CRNT')
  AND d."STATUS_CD" = 'ACTV';


------------------------------------------------------------------DELINQUENT---------------------------------------------------------------------
SELECT * FROM wagedetail.delinquent_emp_staging
WHERE "STATUS_CD" = 'ACTV';


-------------------------------------------------------------------WAGES---------------------------------------------------------------------------
--see all fields--
SELECT * FROM wagedetail.wage_unit_dtl --limit this--
--can use EMPR_ACCT_ID, RPT_YR, RPT_QTR, SSN, 20253(CCYYQ_ID), LAST_NA, FIRST_NA, 

--See all quarters reported for a particular year--
SELECT * FROM wagedetail.wage_dtl_tran
WHERE "RPT_YR" ='2025';

--Search wages by submitted date--
SELECT * FROM wagedetail.wage_dtl_tran
ORDER BY "TRAN_ID" ASC LIMIT 100

--Wages submitted key in on submitted--can search by PAID, UNPD prob--
SELECT *
FROM wagedetail.wage_empr_hdr_h
WHERE "RPT_YR" >= 2024
  AND LEFT("STATUS_CD", 3) = 'SUB';

SELECT * FROM wagedetail.empr_crdt_dtl WHERE "CREDIT_AM" >0--CHANGE AS NEEDED-

--Find Employers with nothing owed and nothing to pay--
SELECT * FROM wagedetail.empr_acct_tran WHERE "OWED_AM" = 00.00
AND "UNPAID_AM" = .00
AND "RPT_YR" = 2024
--Also search by CCYYQ_ID like 20242, EMPR_ACCT_ID--

--Code lookup for wage bulk files--run in main server--
SELECT * FROM core.lookupcode
WHERE "Code" = 'LOAD'

WAGES BULK FILE 
--WONL--Name-wagedtl_source--Description-ONLINE
--ACHO--Name-Payment_Constants--Description-Acho
--WSAV--Name-WAGEDTL_FILING_METHODS--Description-File Upload-and- NAME-WAGEDTL_SOURCE--Description-Save
--LOAD--Name-DHHS_MRS_STATUS--Description-Before DHHS/MRS outbbound file sent data is marked as Load-- and -- Name-WAGEDTL_STATUS_CODE--Description-Loaded File With No Errors--
ORDER by "Code"

--Wages that have been submitted by year and quarter--
SELECT * FROM batch.wage_r1_bridge_outbound_staging
ORDER BY wage_r1_bridge_outbound_staging_id ASC LIMIT 100

--Out of state wages--
SELECT * FROM wagedetail.out_of_state_wages WHERE "RPT_YR" = '2025'
ORDER BY "EMPR_ACCT_ID" ASC, "RPT_YR" ASC, "TAXABLE_SEQ_NU" ASC, "SSN" ASC, "STATE_CD" ASC LIMIT 50000
SELECT * FROM wagedetail.delinquent_emp_staging 
WHERE "STATUS_CD" = 'ACTV';

--Employers with Submitted Wage reports of zero--
SELECT DISTINCT
  e."EMPR_ACCT_ID",
  e."FEIN",
  w."RPT_QTR"
FROM employer.empr_dtl e
JOIN wagedetail.wage_dtl_tran w
  ON w."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
WHERE w."RPT_YR" = 2025
  AND w."STATUS_CD" = 'SUBM'
  AND COALESCE(w."GROSS_WAGES_AM", 0) = 0;

LEFT JOIN wagedetail.wage_dtl_tran w
ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"
AND "RPT_QTR" = '1'
AND "RPT_YR" = '2025'

WHERE w."EMPR_ACCT_ID" IS NULL

--Wage submission search by last name if known--
select * from wagedetail.wage_unit_dtl_h where "LAST_NA" = 'Cox' limit 3

--Unpaid wages--
SELECT * FROM wagedetail.empr_acct_tran
WHERE "UNPAID_AM" > 200000

--Employer owed amount search--
SELECT *
FROM wagedetail.empr_acct_tran
WHERE 
  "OWED_AM" <= 50.50
  AND MOD("OWED_AM" * 100, 10) = 0
LIMIT 20

--No missing wage reports--
Select * FROM employer.terminate_empr_staging
WHERE "is_processed" = 'Y'
AND 'no_missing_wage_reports_in' = 'Y'

--multiple missing wage reports--
c


----------------------------------------------------------------------PAYMENTS------------------------------------------------------------------------

---Employers with exact amount paid or owed--
SELECT * FROM wagedetail.empr_acct_tran WHERE "UNPAID_AM" = .00 limit 10

SELECT * FROM wagedetail.empr_acct_tran WHERE "OWED_AM" <= 50.50 limit 20

--Employer owed amount search--
SELECT *
FROM wagedetail.empr_acct_tran
WHERE 
  "OWED_AM" <= 50.50
  AND MOD("OWED_AM" * 100, 10) = 0
LIMIT 20

--Mainly to see Payment amounts and Payment Status--
SELECT * FROM wagedetail.payment_main
WHERE "STATUS_CD" > 'PAID'
ORDER BY "PAYMENT_NU" ASC LIMIT 10000

--To find payments unpaid/cancelled/rejected--
SELECT * FROM wagedetail.payment_main 
WHERE "SCHEDULED_PAYMENT_IND" = 'N'
AND "EMPR_ACCT_ID" = '200000497';



-------------------------------------------------------------PAYMENT PLANS-------------------------------------------------------------------

--Employers with Payment Plans/or eligible for one--
SELECT * FROM wagedetail.pmt_plan_dtl
WHERE "OWED_AM" > 1000
AND "RPT_YR" > 2024
AND "DOWN_PMT_IN" ='N' LIMIT 20;

--If you have the KEIN--
SELECT * FROM wagedetail.pmt_plan_dtl 
WHERE "EMPR_ACCT_ID" = '200000497'

--Employers with payment plans AND has bankruptcy--
SELECT e.*
     , p.*
FROM employer.empr_dtl e
JOIN wagedetail.pmt_plan_hdr p
  ON e."EMPR_ACCT_ID" = p."EMPR_ACCT_ID"
WHERE e."BNKRPCY_STATE_CD" = 'KY'
  AND e."STATUS_CD" IN ('ACTV', 'RVSD', 'DFLT', 'CLSD', 'CRNT')
  AND p."STATUS_CD" = 'DFLT';

--To find payments unpaid/cancelled/rejected--
SELECT * FROM wagedetail.payment_main 
WHERE "SCHEDULED_PAYMENT_IND" = 'N'
AND "EMPR_ACCT_ID" = '200000497';

--Payment Plans in Processing(not approved yet)--
SELECT * FROM wagedetail.payment_plan_status_staging

--Payment plans in pending status--
SELECT * FROM wagedetail.pmt_plan_hdr
WHERE "STATUS_CD" = 'PEND';

--Payment Plans in default--
SELECT * FROM wagedetail.pmt_plan_hdr
WHERE "STATUS_CD" = 'DFLT';




-------------------------------------------------------------------LEINS AND LEVYS---------------------------------------------------------------------------

--Find Lien--
SELECT * FROM wagedetail.lien_hdr WHERE "LIEN_ISSUE_DT" < '2025-11-01'
AND "LIEN_STATUS_CD" = 'ACTV'
AND "LIEN_AM" > 100
LIMIT 10;

--Lien w green reciept, ex find all active liens after Nov 1 2025 with all green reciepts--
SELECT lh.*, gr.*
FROM wagedetail.lien_hdr lh
LEFT JOIN wagedetail.green_receipt gr
  ON lh."EMPR_ACCT_ID" = gr."empr_acct_id"
WHERE lh."LIEN_ISSUE_DT" > '2025-11-01'
  AND lh."LIEN_STATUS_CD" = 'ACTV'
  AND lh."LIEN_AM" > 100
  AND (gr."green_receipt_fs" = 'Y' OR gr."green_receipt_fs" IS NULL);

--See All--
SELECT * FROM wagedetail.green_receipt LIMIT 5;
SELECT * FROM wagedetail.lien_hdr LIMIT 5;

--LEVY Status--
SELECT * FROM wagedetail.levy_hdr
WHERE "LEVY_STATUS_CD" = 'CLSD'; -- can use 'OPEN' and maybe 'PEND'--


----------------------------------------------------------------CORRESPONDENCE-------------------------------------------------------------------------

--Find correspondences--
SELECT * FROM correspondence.corres_tax--commonpresit
WHERE "STATUS_CD" = 'PEND';

--eng language find code--
en-us

--Find correspondence by ID to get batch parameters--
SELECT * FROM correspondence.corres_tax_h
WHERE "CORRESPONDENCE_ID" = '41505'; ---replace with your actual Document ID--

--EMPLOYER CORRESPONDENCE PREFERENCE--
--codes are PAPR and ELEC--
--example-- 
SELECT * FROM employer.empr_prfrnc 
WHERE "BENEFIT_CORRES_TYPE_CD" ='elec' LIMIT 1000

--Find Correspondences by the ID-- Contains parameters needed for batch runs like timestamp and template ID--
SELECT * FROM correspondence.corres_tax_h
WHERE "CORRESPONDENCE_ID" = '17801'; ---replace with your actual Document ID--

--Pending correspondences--
SELECT * FROM correspondence.corres_tax--commonpresit
 WHERE "EMPR_ACCT_ID" = ---KEIN
 WHERE "STATUS_CD" = 'PEND'
 
--Find all the correspondence templates---
SELECT * FROM correspondence.rsrc_templates

--Find status of correspondence--
SELECT * FROM correspondence.corres_tax
WHERE "EMPR_ACCT_ID" = '100000252'

--Deepr Dive Correspondence--
SELECT *
FROM correspondence.corres_tax
WHERE "STATUS_CD" = 'PEND'
  AND "OUTPUT_TYPE_CD" = 'ELEC'
ORDER BY "EMPR_ACCT_ID" DESC
LIMIT 5;

--------------------------------------------
--Finds Contruction coded naics(23) and filters by kein and if the account is active--
SELECT
    r."NAICS_CD",
    e."EMPR_ACCT_ID",
    r."STATUS_CD" AS active_status
FROM
    employer.rgst_empr r
JOIN
    employer.empr_dtl e
    ON r."EMPR_RGST_ID" = e."EMPR_RGST_ID"
WHERE
    substring(r."NAICS_CD" FROM 1 FOR 2) = '23'
LIMIT 20;
-------------------------------------------


SELECT * FROM wagedetail.chrg_Dtl LIMIT 10
---------------------------

--------------------------------------------RATES--------------------------------------------
--Tax rate easy--
SELECT * FROM employer.tax_rate_hdr_h
ORDER BY "TAX_RATE_ID" ASC, "UPDATE_NU" ASC LIMIT 100

--Find rate and more details--
SELECT * FROM employer.tax_rate_dtl
ORDER BY "TAX_RATE_DTL_ID" ASC LIMIT 100

SELECT * FROM employer.tax_rate_params
ORDER BY "RPT_YR" ASC, "EFF_BEGIN_DT" ASC, "RATE_STATUS_CD" ASC, "EFF_END_DATE" ASC LIMIT 100

--------------------------------------------TPS-------------------------------------------------
--Reports created within the portal--
select * from tps.rpt_tps_universe where "RPT_TYPE_CD"='SUCC' 
order by "CREATE_DT" asc;
--
select * from tps.rpt_tps_rqc where "RPT_CD"='SUCC';
--
SELECT * FROM tps.tps_workplan 
WHERE "RPT_YR" = '2025'
--
SELECT * FROM tps.tps_universe_sample_matrix

------------------
SELECT COUNT(*) AS employers_in_cohort
FROM employer.empr_dtl d
WHERE d."LIABILITY_INCURRED_DT" = DATE '2024-01-01'
  AND d."AGENT_IN" = 'N'
  AND d."STATUS_CD" = 'INAC';

 ----------------------
 SELECT h."STATUS_CD", COUNT(*) AS cnt
FROM wagedetail.wage_empr_hdr_h h
WHERE h."RPT_YR" >= 2024
GROUP BY h."STATUS_CD"
ORDER BY cnt DESC;


-------------------------------------
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e
WHERE e."LIABILITY_INCURRED_DT"::date = DATE '2024-01-01'
  AND e."AGENT_IN" = 'N'
  AND e."STATUS_CD" = 'INAC'
  AND NOT EXISTS (
    SELECT 1
    FROM wagedetail.wage_dtl_tran w
    WHERE w."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
      AND w."RPT_YR" BETWEEN 2024 AND 2025
  )
ORDER BY e."EMPR_ACCT_ID"
LIMIT 100;
-------------------------------------
Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01'
-----------------------------------------


-----------------------------------------------------------AUDIT------------------------------------------------------------------------
--Basic lookup--
SELECT * FROM fieldaudit.audit LIMIT 50
--Search by AUDIT_YR,EMPR_ACCT_ID,SELECTION_CD(SPFC,RDNM,TRGT)--HUGE way--

--Under Audit and delinquent Quarters--
WITH supervisor_auditor AS (

    SELECT CONCAT(ccu."FIRST_NA", ' ', ccu."LAST_NA") AS auditor_name

    FROM tax_fieldaudit_fdw.audit_user ads

    LEFT JOIN common_core_fdw.user ccu ON ads."USER_ID" = ccu."USER_ID"

    WHERE ads."AUDITOR_TYPE_CD" = 'SUPE' AND ads."ACTIVE_IN" = 'A'

    ORDER BY ccu."LAST_NA", ccu."FIRST_NA"

    LIMIT 1

),

admin_auditor AS (

    SELECT CONCAT(ccu."FIRST_NA", ' ', ccu."LAST_NA") AS auditor_name

    FROM tax_fieldaudit_fdw.audit_user ads

    LEFT JOIN common_core_fdw.user ccu ON ads."USER_ID" = ccu."USER_ID"

    WHERE ads."AUDITOR_TYPE_CD" = 'ADMN' AND ads."ACTIVE_IN" = 'A'

    ORDER BY ccu."LAST_NA", ccu."FIRST_NA"

    LIMIT 1

)

SELECT 

    des."EMPR_ACCT_ID" as "KEIN",

    ed."ENTITY_NA" AS "Employer Name",

    COUNT(*) AS "Count of Delinquent Quarters",

    addr."ZIP" AS "ZIP",

    COALESCE(

        NULLIF(CONCAT(ccu."FIRST_NA", ' ', ccu."LAST_NA"), ' '),

        NULLIF((SELECT auditor_name FROM supervisor_auditor),' '),

        (SELECT auditor_name FROM admin_auditor)

    ) AS "Auditor's Name",

    STRING_AGG(

        LPAD(des."RPT_QTR"::TEXT, 2, '0') || '-' || des."RPT_YR"::TEXT,

        ','

        ORDER BY des."RPT_YR" ASC, des."RPT_QTR" ASC

    ) AS "Delinquent Quarters"

FROM tax_wagedetail_fdw.delinquent_emp_staging des

LEFT JOIN tax_employer_fdw.empr_dtl ed ON des."EMPR_ACCT_ID" = ed."EMPR_ACCT_ID"

LEFT JOIN tax_employer_fdw.address addr ON des."EMPR_ACCT_ID" = addr."EMPR_ACCT_ID"

    AND addr."ADDR_TYPE_CD" = 'PHYS'

    AND addr."STATUS_CD" = 'ACTV'

    AND addr."EMPR_UNIT_ID" = 0

LEFT JOIN tax_fieldaudit_fdw.audit_usr_zip auz ON addr."ZIP" = auz."ZIP"

LEFT JOIN common_core_fdw.user ccu ON auz."USER_ID" = ccu."USER_ID"

INNER JOIN (

    SELECT "parameter_value"

    FROM reports.config_table 

    WHERE "report_name"='Delinquent Wage Reports - Details' AND "column_name"='State_Code'

) c ON addr."STATE_CD"=c."parameter_value"

WHERE des."STATUS_CD" = 'ACTV'

GROUP BY des."EMPR_ACCT_ID", ed."ENTITY_NA", addr."ZIP", ccu."FIRST_NA", ccu."LAST_NA"

ORDER BY des."EMPR_ACCT_ID"

--------------------------------------PEO and TPA---------------------------------
--PEO SEARCH--
SELECT  "PEO_RATING_CD","PEO_IN","PEO_AGENT_ID", *
FROM employer.empr_dtl limit 10
WHERE "EMPR_ACCT_ID" = '200003729'

--Another way--
SELECT  "PEO_RATING_CD","CLIENT_LEASING_IN","PEO_IN",*
FROM employer.empr_dtl
WHERE "PEO_IN" = 'N' and "CLIENT_LEASING_IN" = 'Y' AND "PEO_RATING_CD"='GRPR'
ORDER BY "PEO_IN" DESC
LIMIT 1000000;
--Another way--
Select * from employer.empr_dtl where "PEO_IN" = 'Y'
SELECT * FROM employer.agent LIMIT 50000

--TPA Search--
SELECT * FROM employer.rgst_agent WHERE "AGENT_NA" = 'Peter Parker'
--or search in order for TPA if dont know any info--
SELECT * FROM employer.rgst_agent
ORDER BY "AGENT_RGST_ID" ASC LIMIT 100


--------------------------------------------------One offs and projects----------------------------------------------------------
--Find Common columns throughout tables for JOIN--
SELECT a.column_name
FROM information_schema.columns a
JOIN information_schema.columns b
  ON b.column_name = a.column_name
WHERE a.table_schema = 'employer'
  AND a.table_name   = 'empr_dtl'
  AND b.table_schema = 'wagedetail'
  AND b.table_name   = 'wage_empr_hdr_h'
ORDER BY 1;

--Find the character code for anything--
SELECT * FROM core.lookupcode --run in commonpresit

--Find language preference by language--commonpresit
SELECT * FROM correspondence.corres_tax
WHERE "LANGUAGE" = 'es-es'
--eng language find code--
en-us

--Employer user logins--
SELECT * FROM automation.employer_user;

--Total Employers by language Preference--
SELECT "LANGUAGE_PREFERENCE", COUNT(*) AS cnt
FROM employer.address
GROUP BY "LANGUAGE_PREFERENCE"
ORDER BY cnt DESC;

--Employer Event Logs--
SELECT * FROM employer.event_log
 WHERE 'staffuserid' is not null
ORDER BY event_log ASC LIMIT 300

SELECT * FROM core."user" 
 WHERE "USER_ID" = 'S-Staff.User115@labids.dslab.ky.gov'--Run in commonpresit

--Wages submitted till current quarter and UI Contribution Payments made and has current Penalty or Interest payments--
SELECT 
w."EMPR_ACCT_ID",
MAX(w."CCYYQ_ID") as last_filed_period,
a."OWED_AM" as tax_owed,
SUM(i."INTEREST_AM") as interest_due
FROM wagedetail.wage_dtl_tran w
JOIN wagedetail.empr_acct_due a ON w."EMPR_ACCT_ID" = a."EMPR_ACCT_ID"
JOIN wagedetail.int_calc_dtl i ON w."EMPR_ACCT_ID" = i."EMPR_ACCT_ID"
WHERE 
-- 1. Must have filed for the most recent cycle
w."CCYYQ_ID" = 20254
-- 2. Tax/Contribution balance MUST be zero
AND a."OWED_AM" = 0.00
-- 3. But interest must be outstanding
AND i."INTEREST_AM" > 0.00
GROUP BY 
w."EMPR_ACCT_ID", a."OWED_AM";

--Employer Units--
SELECT * FROM EMPLOYER.EMPR_UNIT WHERE "EMPR_UNIT_ID" >0 
 AND "STATUS_CD" = 'ACTV' LIMIT 4000

--NAIC


---------------------------------------Needs updates----------------------------
sELECT * FROM wagedetail.wage_empr_hdr
WHERE 'status_cd' LIKE 'SUB%'
AND "RPT_YR" = 2022


 SELECT * FROM wagedetail.wagesummary_outgoing_stage
WHERE "NUMBER_OF_EMPLOYEES" > '2';


  ------------------------------might be dups-----------
  -- Find employer details--
--------BASE LOOKUPS---------
-employer.empr_dtl_h and employer.empr_dtl
SELECT * FROM employer.empr_dtl limit 500 WHERE "EMPR_ACCT_ID" = 200000793;
---OR CONVERTED EMPLOYERS--
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e
LEFT JOIN wagedetail.wage_dtl_tran w
ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"
AND "RPT_QTR" = '1'
AND "RPT_YR" = '2024'
WHERE w."EMPR_ACCT_ID" IS NULL
and e."STATUS_CD" = 'ACTV'
Limit 1000

---Inactive and Active Employere---
SELECT * FROM employer.empr_dtl
WHERE "STATUS_CD" = 'inac'
limit 5;
---------------------
--LIABILITY DATE--
Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01'
----------------------
--CONTACT INFO LIKE PH NUMBERS AND F/L NAMES--
SELECT * FROM employer.empr_cntct LIMIT 5
--EMPLOYER CORRESPONDENCE PREFERENCE--
--codes are PAPR and ELEC--
--example-- 
SELECT * FROM employer.empr_prfrnc 
WHERE "BENEFIT_CORRES_TYPE_CD" ='elec' LIMIT 1000
--LANGUAGE PREF-----
SELECT * FROM employer.address WHERE "LANGUAGE_PREFERENCE" = 'sp-us'
SELECT * FROM employer.rgst_addr WHERE "LANGUAGE_PREFERENCE" > 'en'
--------------------------------------------------
SELECT * FROM employer.bankruptcy_dtl;

--Employers with Bankruptcies--
SELECT * FROM employer.empr_dtl 
WHERE "BNKRPCY_STATE_CD" = 'KY'
AND "STATUS_CD" = 'ACTV';
----------------------------------------------------
--To find payments unpaid/cancelled/rejected--
SELECT * FROM wagedetail.payment_main 
WHERE "SCHEDULED_PAYMENT_IND" = 'N'
AND "EMPR_ACCT_ID" = '200000497';
-----------------------------------------------------
SELECT * FROM wagedetail.payment_plan_status_staging

--Find Employers with Payment Plans/or eligible for one--
SELECT * FROM wagedetail.pmt_plan_dtl
WHERE "OWED_AM" > 1000
AND "RPT_YR" > 2025
AND "DOWN_PMT_IN" ='N' LIMIT 20;
-------------------------------------------------------
SELECT * FROM wagedetail.pmt_plan_hdr
WHERE "STATUS_CD" = 'PEND';
----------------------------------------------------------
--Employers with payment plans and bankruptcies--
SELECT e.*
     , p.*
FROM employer.empr_dtl e
JOIN wagedetail.pmt_plan_hdr p
  ON e."EMPR_ACCT_ID" = p."EMPR_ACCT_ID"
WHERE e."BNKRPCY_STATE_CD" = 'KY'
  AND e."STATUS_CD" IN ('ACTV', 'RVSD', 'DFLT', 'CLSD', 'CRNT')
  AND p."STATUS_CD" = 'DFLT';
-------------------------------------------------------------------
SELECT * FROM wagedetail.pmt_plan_hdr
WHERE "STATUS_CD" = 'DFLT';
-------------------------------------------------------------------
SELECT * FROM wagedetail.wage_unit_dtl;

SELECT * FROM wagedetail.wage_dtl_tran
WHERE "RPT_YR" ='2025';
---------------------------------------------------------------------
--Terminated Employer in process--
Select * FROM employer.terminate_empr_staging
WHERE "is_processed" = 'Y';

Select * FROM employer.terminate_empr_staging
WHERE "is_processed" = 'Y'
AND 'no_missing_wage_reports_in' = 'Y';
--------------------------------------------------------------------------
-------WAGES-----------
--see all fields--
SELECT * FROM wagedetail.wage_unit_dtl
-----------------------------------------
SELECT * FROM wagedetail.delinquent_emp_staging 
WHERE "STATUS_CD" = 'ACTV';

--Delinquent multiple quarters--
SELECT * FROM wagedetail.delinquent_emp_staging WHERE "RPT_QTR" IN (1,2,3,4)
ORDER BY "EMPR_ACCT_ID" ASC LIMIT 10000
------------------------------------------------------------------------
--Find employers who are delinquent and in bankruptcy--
SELECT e.*
     , d.*
FROM employer.empr_dtl e
JOIN wagedetail.delinquent_emp_staging d
  ON e."EMPR_ACCT_ID" = d."EMPR_ACCT_ID"
WHERE e."BNKRPCY_STATE_CD" = 'KY'
  AND e."STATUS_CD" IN ('ACTV', 'RVSD', 'DFLT', 'CLSD', 'CRNT')
  AND d."STATUS_CD" = 'ACTV';

 SELECT * FROM wagedetail.wagesummary_outgoing_stage
WHERE "NUMBER_OF_EMPLOYEES" > '2';

SELECT * FROM wagedetail.delinquent_emp_staging
WHERE "STATUS_CD" = 'ACTV';
------------------------------------------------------------------------
--Find employers in delinquency for 8 consecutive quarters--
WITH periods AS (

    SELECT 4 AS "RPT_QTR", 2025 AS "RPT_YR" UNION

    SELECT 3, 2025 UNION

    SELECT 2, 2025 UNION

    SELECT 1, 2025 UNION

    SELECT 4, 2024 UNION

    SELECT 3, 2024 UNION

    SELECT 2, 2024 UNION

    SELECT 1, 2024 UNION

    SELECT 4, 2023 UNION

    SELECT 3, 2023

)

SELECT

    e."EMPR_ACCT_ID",

    e."FEIN",

    p."RPT_QTR",

    p."RPT_YR"

FROM employer.empr_dtl e

CROSS JOIN periods p

LEFT JOIN wagedetail.wage_dtl_tran w

    ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"

    AND w."RPT_QTR" = p."RPT_QTR"

    AND w."RPT_YR" = p."RPT_YR"

WHERE w."EMPR_ACCT_ID" IS NULL

  AND e."STATUS_CD" = 'ACTV'

ORDER BY e."EMPR_ACCT_ID", p."RPT_YR" DESC, p."RPT_QTR" DESC limit 100;
 
WITH periods AS (

    SELECT 4 AS "RPT_QTR", 2025 AS "RPT_YR" UNION

    SELECT 3, 2025 UNION

    SELECT 2, 2025 UNION

    SELECT 1, 2025 UNION

    SELECT 4, 2024 UNION

    SELECT 3, 2024 UNION

    SELECT 2, 2024 UNION

    SELECT 1, 2024 UNION

    SELECT 4, 2023 UNION

    SELECT 3, 2023

),

missing_reports AS (

    SELECT

        e."EMPR_ACCT_ID",

        e."FEIN",

        p."RPT_QTR",

        p."RPT_YR"

    FROM employer.empr_dtl e

    CROSS JOIN periods p

    LEFT JOIN wagedetail.wage_dtl_tran w

        ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"

        AND w."RPT_QTR" = p."RPT_QTR"

        AND w."RPT_YR" = p."RPT_YR"

    WHERE e."STATUS_CD" = 'REAC'

      AND w."EMPR_ACCT_ID" IS NULL

)

SELECT

    m."EMPR_ACCT_ID",

    m."FEIN"

FROM missing_reports m

GROUP BY m."EMPR_ACCT_ID", m."FEIN"

HAVING COUNT(*) = 10

ORDER BY m."EMPR_ACCT_ID" limit 100;
------------------------------------------------------------------------
 --Find Legacy Employers--
SELECT * FROM taxpresit_old_employer_fdw.legacy_employer_xref LIMIT 5
WHERE "empr_acct_id" = '100112708'
------
 SELECT * FROM employer.legacy_employer_xref
 WHERE "empr_acct_id" = '100157808'
-------------------------------------------------------------------------
 --Multiple Reporting Units all columns--
 WITH counted AS (
  SELECT
    A.*,
    B.*,
    COUNT(*) OVER (PARTITION BY A."EMPR_ACCT_ID", B."EMPR_UNIT_ID") AS unit_count
  FROM
    employer.empr_dtl A
    JOIN wagedetail.wage_unit_dtl B
      ON A."EMPR_ACCT_ID" = B."EMPR_ACCT_ID"
  WHERE
    A."STATUS_CD" = 'ACTV'
    AND B."RPT_YR" = 2025 
    AND B."RPT_QTR" = 1
    AND B."EMPR_UNIT_ID" != 0
)
SELECT *
FROM counted
WHERE unit_count > 1
ORDER BY unit_count DESC
LIMIT 10;
--OR--
select A."EMPR_ACCT_ID", b."EMPR_UNIT_ID", COUNT(b.*) from employer.empr_dtl A JOIN wagedetail.wage_unit_dtl b 
on A."EMPR_ACCT_ID" = b."EMPR_ACCT_ID"
where A."STATUS_CD" = 'ACTV'
AND b."RPT_YR" = 2025 
AND B."RPT_QTR" = 1
AND b."EMPR_UNIT_ID" != 0
GROUP BY A."EMPR_ACCT_ID", b."EMPR_UNIT_ID"
HAVING COUNT(b.*) > 1 
ORDER BY 3 DESC
LIMIT 10 ;
----------------------------------------------------------------------
--Find Lien--
SELECT * FROM wagedetail.lien_hdr WHERE "LIEN_ISSUE_DT" < '2025-11-01'
AND "LIEN_STATUS_CD" = 'ACTV'
AND "LIEN_AM" > 100
LIMIT 10;
----------------------------------------------------------------------
--Lien w green reciept, ex find all active liens after Nov 1 2025 with all green reciepts--
SELECT lh.*, gr.*
FROM wagedetail.lien_hdr lh
LEFT JOIN wagedetail.green_receipt gr
  ON lh."EMPR_ACCT_ID" = gr."empr_acct_id"
WHERE lh."LIEN_ISSUE_DT" > '2025-11-01'
  AND lh."LIEN_STATUS_CD" = 'ACTV'
  AND lh."LIEN_AM" > 100
  AND (gr."green_receipt_fs" = 'Y' OR gr."green_receipt_fs" IS NULL);

SELECT * FROM wagedetail.green_receipt LIMIT 5;
SELECT * FROM wagedetail.lien_hdr LIMIT 5;
-------------------------------------------------------

SELECT * FROM wagedetail.levy_hdr
WHERE "LEVY_STATUS_CD" = 'CLSD';
--------------------------------------------------------------------
SELECT * FROM automation.employer_user;
---------------------------------------------
--Find correspondence by ID to get batch parameters--
SELECT * FROM correspondence.corres_tax_h
WHERE "CORRESPONDENCE_ID" = '41505'; ---replace with your actual Document ID--
---------------------------------------------------------
---Employers with exact amount paid or owed--Run in taxpresit
SELECT * FROM wagedetail.empr_acct_tran WHERE "UNPAID_AM" = .00 limit 10

SELECT * FROM wagedetail.empr_acct_tran WHERE "OWED_AM" = 00.00
AND "UNPAID_AM" = .00
AND "RPT_YR" = 2024
--thi
SELECT *
FROM wagedetail.empr_acct_tran
WHERE 
  "OWED_AM" <= 50.50
  AND MOD("OWED_AM" * 100, 10) = 0
LIMIT 20
----------------------------------------
--Find correspondences--
SELECT * FROM correspondence.corres_tax--commonpresit
WHERE "STATUS_CD" = 'PEND';
--eng language find code--
en-us
-------------------------------------------------
--Find Correspondences by the ID -- Contains parameters needed for batch runs like timestamp and template ID--
SELECT * FROM correspondence.corres_tax_h
WHERE "CORRESPONDENCE_ID" = '17801'; ---replace with your actual Document ID--
------------------------------------------------------
--Find the character code for anything--
SELECT * FROM core.lookupcode --run in commonpresit
--------------------------------------------------------
--Find employers eligible for manual termination--taxpresit--
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e

LEFT JOIN wagedetail.wage_dtl_tran w
ON e."EMPR_ACCT_ID" = w."EMPR_ACCT_ID"
AND "RPT_QTR" = '1'
AND "RPT_YR" = '2025'

WHERE w."EMPR_ACCT_ID" IS NULL
--------------------------------------------
--Finds Contruction coded naics(23) and filters by kein and if the account is active--
SELECT
    r."NAICS_CD",
    e."EMPR_ACCT_ID",
    r."STATUS_CD" AS active_status
FROM
    employer.rgst_empr r
JOIN
    employer.empr_dtl e
    ON r."EMPR_RGST_ID" = e."EMPR_RGST_ID"
WHERE
    substring(r."NAICS_CD" FROM 1 FOR 2) = '23'
LIMIT 20;
-------------------------------------------

--Find all the correspondence templates---
SELECT * FROM correspondence.rsrc_templates

--Find status of correspondence--commonpresit--
SELECT * FROM correspondence.corres_tax
WHERE "EMPR_ACCT_ID" = '200000497'
-----------------------
---Expereince Transfer---
SELECT * FROM employer.expr_trnsfr_hdr LIMIT 10
------------------

SELECT * FROM wagedetail.pmt_plan_dtl 
WHERE "EMPR_ACCT_ID" = '200000497'
------------------------------------------
--Terminated employer--taxpresit
SELECT "TERMINATION_REASON_CD", * FROM EMPLOYER.EMPR_SUSPEND
WHERE "EMPR_ACCT_ID" IN (200001996) -- can add more keins, seperate with commma--
----------------------------------
--WANT TO KNOW HOW MANY SPANISH EMPLOYERS WE HAVE?--commonpresit
SELECT "LANGUAGE_PREFERENCE", COUNT(*) AS cnt
FROM employer.address
GROUP BY "LANGUAGE_PREFERENCE"
ORDER BY cnt DESC;

--WANT TO FIND THOSE EMPLOYERS?--commonpresit
SELECT * FROM correspondence.corres_tax
WHERE "LANGUAGE" = 'es-es'

--eng language find code--
en-us
----------------------------------------
------------------------------------

SELECT * FROM wagedetail.chrg_Dtl LIMIT 10
---------------------------

Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01'
AND 'AGENT_IN' = 'N'
AND 'STATUS_CD' = 'INAC'


sELECT * FROM wagedetail.wage_empr_hdr_h
WHERE 'status_cd' LIKE 'SUB%'


"RPT_YR" = 2024

--Quick way to find candidate join columns (overlapping names)--
SELECT a.column_name
FROM information_schema.columns a
JOIN information_schema.columns b
  ON b.column_name = a.column_name
WHERE a.table_schema = 'employer'
  AND a.table_name   = 'empr_dtl'
  AND b.table_schema = 'wagedetail'
  AND b.table_name   = 'wage_empr_hdr_h'
ORDER BY 1;
--------------------------------------
SELECT *
FROM wagedetail.wage_empr_hdr_h
WHERE "RPT_YR" >= 2024
  AND LEFT("STATUS_CD", 3) = 'SUB';
------------------
SELECT COUNT(*) AS employers_in_cohort
FROM employer.empr_dtl d
WHERE d."LIABILITY_INCURRED_DT" = DATE '2024-01-01'
  AND d."AGENT_IN" = 'N'
  AND d."STATUS_CD" = 'INAC';

 ----------------------
 SELECT h."STATUS_CD", COUNT(*) AS cnt
FROM wagedetail.wage_empr_hdr_h h
WHERE h."RPT_YR" >= 2024
GROUP BY h."STATUS_CD"
ORDER BY cnt DESC;


-------------------------------------
SELECT e."EMPR_ACCT_ID", e."FEIN"
FROM employer.empr_dtl e
WHERE e."LIABILITY_INCURRED_DT"::date = DATE '2024-01-01'
  AND e."AGENT_IN" = 'N'
  AND e."STATUS_CD" = 'INAC'
  AND NOT EXISTS (
    SELECT 1
    FROM wagedetail.wage_dtl_tran w
    WHERE w."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
      AND w."RPT_YR" BETWEEN 2024 AND 2025
  )
ORDER BY e."EMPR_ACCT_ID"
LIMIT 100;
-------------------------------------
Select * from employer.empr_dtL where "LIABILITY_INCURRED_DT" = '2024-01-01'
-----------------------------------------

----FIND EMPLOYERS WITH WAGE REPORTS AND PAYMENT DUE AND HAS TPA-------
WITH tpa_map AS (
  SELECT
    ae."EMPR_ACCT_ID",
    COUNT(DISTINCT ae."AGENT_ID") AS tpa_cnt,
    MIN(ae."AGENT_ID")            AS primary_agent_id,
    STRING_AGG(DISTINCT ae."AGENT_ID"::text, ',' ORDER BY ae."AGENT_ID"::text) AS agent_ids_csv
  FROM employer.agent_empr ae
  GROUP BY 1
),
unpaid_by_qtr AS (
  SELECT
    t."EMPR_ACCT_ID",
    t."RPT_YR",
    t."RPT_QTR",
    SUM(COALESCE(t."UNPAID_AM", 0)) AS total_unpaid_am,
    SUM(COALESCE(t."OWED_AM", 0))   AS total_owed_am,
    MIN(t."DUE_DT")                 AS oldest_due_dt,
    MAX(t."DUE_DT")                 AS newest_due_dt
  FROM wagedetail.empr_acct_tran t
  WHERE COALESCE(t."UNPAID_AM", 0) > 0
  GROUP BY 1,2,3
),
missing_rpt_by_qtr AS (
  SELECT DISTINCT
    d."EMPR_ACCT_ID",
    d."RPT_YR",
    d."RPT_QTR"
  FROM wagedetail.delinquent_emp_staging d
  WHERE TRIM(d."STATUS_CD") = 'ACTV'
    AND d."MISSING_RPT_IN" = TRUE
),
employers_with_both AS (
  SELECT u."EMPR_ACCT_ID"
  FROM (SELECT DISTINCT "EMPR_ACCT_ID" FROM unpaid_by_qtr) u
  INNER JOIN (SELECT DISTINCT "EMPR_ACCT_ID" FROM missing_rpt_by_qtr) m
    ON m."EMPR_ACCT_ID" = u."EMPR_ACCT_ID"
),
overlap_qtrs AS (
  SELECT
    u."EMPR_ACCT_ID",
    u."RPT_YR",
    u."RPT_QTR"
  FROM unpaid_by_qtr u
  JOIN missing_rpt_by_qtr m
    ON m."EMPR_ACCT_ID" = u."EMPR_ACCT_ID"
   AND m."RPT_YR"       = u."RPT_YR"
   AND m."RPT_QTR"      = u."RPT_QTR"
),
rollup AS (
  SELECT
    e."EMPR_ACCT_ID",

    -- TPA tie
    tm.primary_agent_id,
    tm.tpa_cnt,
    tm.agent_ids_csv,

    -- payment due rollups
    SUM(u.total_unpaid_am) AS payment_due_unpaid_am,
    SUM(u.total_owed_am)   AS payment_due_owed_am,
    COUNT(DISTINCT (u."RPT_YR"::text || '-' || u."RPT_QTR"::text)) AS qtrs_with_payment_due,
    MIN(u.oldest_due_dt)   AS oldest_payment_due_dt,
    MAX(u.newest_due_dt)   AS newest_payment_due_dt,

    -- missing report rollups
    COUNT(DISTINCT (m."RPT_YR"::text || '-' || m."RPT_QTR"::text)) AS qtrs_missing_wage_rpt,

    -- overlap
    COUNT(DISTINCT (o."RPT_YR"::text || '-' || o."RPT_QTR"::text)) AS qtrs_with_both_conditions
  FROM employers_with_both e
  JOIN tpa_map tm
    ON tm."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"          -- enforces "tied to a TPA"
  LEFT JOIN unpaid_by_qtr u
    ON u."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
  LEFT JOIN missing_rpt_by_qtr m
    ON m."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
  LEFT JOIN overlap_qtrs o
    ON o."EMPR_ACCT_ID" = e."EMPR_ACCT_ID"
  GROUP BY
    e."EMPR_ACCT_ID",
    tm.primary_agent_id,
    tm.tpa_cnt,
    tm.agent_ids_csv
)
SELECT
  "EMPR_ACCT_ID",
  primary_agent_id AS "AGENT_ID",
  tpa_cnt,
  agent_ids_csv,
  payment_due_unpaid_am,
  payment_due_owed_am,
  qtrs_with_payment_due,
  qtrs_missing_wage_rpt,
  qtrs_with_both_conditions,
  (qtrs_with_both_conditions > 0) AS has_same_qtr_overlap,
  oldest_payment_due_dt,
  newest_payment_due_dt
FROM rollup
ORDER BY payment_due_unpaid_am DESC, oldest_payment_due_dt ASC;