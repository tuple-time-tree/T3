CREATE SCHEMA consumer;
DROP TABLE IF EXISTS consumer."EXPENDITURES";

CREATE TABLE consumer."EXPENDITURES" (
                                "EXPENDITURE_ID" varchar(8) ,
                                "HOUSEHOLD_ID" varchar(8) ,
                                "YEAR" integer ,
                                "MONTH" integer ,
                                "PRODUCT_CODE" varchar(6) ,
                                "COST" double precision ,
                                "GIFT" integer ,
                                "IS_TRAINING" integer ,
                                PRIMARY KEY ("EXPENDITURE_ID")
) ;

DROP TABLE IF EXISTS consumer."HOUSEHOLDS";

CREATE TABLE consumer."HOUSEHOLDS" (
                              "HOUSEHOLD_ID" varchar(8) ,
                              "YEAR" integer ,
                              "INCOME_RANK" double precision ,
                              "INCOME_RANK_1" double precision ,
                              "INCOME_RANK_2" double precision ,
                              "INCOME_RANK_3" double precision ,
                              "INCOME_RANK_4" double precision ,
                              "INCOME_RANK_5" double precision ,
                              "INCOME_RANK_MEAN" double precision ,
                              "AGE_REF" integer ,
                              PRIMARY KEY ("HOUSEHOLD_ID")
) ;

DROP TABLE IF EXISTS consumer."HOUSEHOLD_MEMBERS";

CREATE TABLE consumer."HOUSEHOLD_MEMBERS" (
                                     "HOUSEHOLD_ID" varchar(8) ,
                                     "YEAR" integer ,
                                     "MARITAL" varchar(1) ,
                                     "SEX" varchar(1) ,
                                     "AGE" integer ,
                                     "WORK_STATUS" varchar(2) DEFAULT NULL
) ;
