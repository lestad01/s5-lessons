-- CREATE TABLE CDM.DM_SETTLEMENT_REPORT 
-- (
--     ID SERIAL NOT NULL,
--     RESTAURANT_ID VARCHAR NOT NULL,
--     RESTAURANT_NAME VARCHAR NOT NULL,
--     SETTLEMENT_DATE DATE NOT NULL,
--     ORDERS_COUNT INTEGER NOT NULL,
--     ORDERS_TOTAL_SUM NUMERIC(14,2) NOT NULL,
--     ORDERS_BONUS_PAYMENT_SUM NUMERIC(14,2) NOT NULL,
--     ORDERS_BONUS_GRANTED_SUM NUMERIC(14,2) NOT NULL,
--     ORDER_PROCESSING_FEE NUMERIC(14,2) NOT NULL,
--     RESTAURANT_REWARD_SUM NUMERIC(14,2) NOT NULL
-- )
-- ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT SETTLEMENT_REPORT_PK PRIMARY KEY(ID)
-- ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT dm_settlement_report_settlement_date_check CHECK (SETTLEMENT_DATE >= date'2022-01-01' and SETTLEMENT_DATE < date'2500-01-01')
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_ORDERS_COUNT CHECK (ORDERS_COUNT >= 0);
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_ORDERS_TOTAL_SUM CHECK (ORDERS_TOTAL_SUM >= 0);
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_ORDERS_BONUS_PAYMENT_SUM CHECK (ORDERS_BONUS_PAYMENT_SUM >= 0);
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_ORDERS_BONUS_GRANTED_SUM CHECK (ORDERS_BONUS_GRANTED_SUM >= 0);
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_ORDER_PROCESSING_FEE CHECK (ORDER_PROCESSING_FEE >= 0);
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ADD CONSTRAINT CHECK_RESTAURANT_REWARD_SUM CHECK (RESTAURANT_REWARD_SUM >= 0);

ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN ORDERS_COUNT SET DEFAULT 0;
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN ORDERS_TOTAL_SUM SET DEFAULT 0;
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN ORDERS_BONUS_PAYMENT_SUM SET DEFAULT 0;
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN ORDERS_BONUS_GRANTED_SUM SET DEFAULT 0;
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN ORDER_PROCESSING_FEE SET DEFAULT 0;
ALTER TABLE CDM.DM_SETTLEMENT_REPORT ALTER COLUMN RESTAURANT_REWARD_SUM SET DEFAULT 0;