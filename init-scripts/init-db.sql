-- Create the database
-- CREATE DATABASE DEVDB AUTOMATIC STORAGE YES USING CODESET UTF-8 TERRITORY US PAGESIZE 32768;

-- Connect to the database
-- CONNECT TO DEVDB;

-- -- Create a schema
-- CREATE SCHEMA sail;

SET SCHEMA sail;

CREATE TABLE gp_event_reformatted (
    ALF_E BIGINT NOT NULL,
    ALF_STS_CD CHAR(1) NOT NULL,
    ALF_MTCH_PCT DECIMAL(5,2) NOT NULL,
    PRAC_CD_E INTEGER NOT NULL,
    EVENT_CD_ID INTEGER NOT NULL,
    EVENT_VAL DECIMAL(10,2) NOT NULL,
    EVENT_DT DATE NOT NULL,
    EVENT_YR SMALLINT NOT NULL
);

COMMENT ON TABLE gp_event_reformatted IS 'Reformatted GP event data table';
COMMENT ON COLUMN gp_event_reformatted.ALF_E IS 'Encrypted anonymised linking field';
COMMENT ON COLUMN gp_event_reformatted.ALF_STS_CD IS 'Status code assigned when deriving the encrypted anonymised linking field';
COMMENT ON COLUMN gp_event_reformatted.ALF_MTCH_PCT IS 'Anonymised linking field match percentage';
COMMENT ON COLUMN gp_event_reformatted.PRAC_CD_E IS 'Encrypted code of patient’s registered General Practice';
COMMENT ON COLUMN gp_event_reformatted.EVENT_CD_ID IS 'Links GP_EVENT_REFORMATTED table with GP_EVENT_CODES table';
COMMENT ON COLUMN gp_event_reformatted.EVENT_VAL IS 'Value associated with the EVENT_CD found in GP_EVENT_CODES table';
COMMENT ON COLUMN gp_event_reformatted.EVENT_DT IS 'Date of the event/incident/medical episode';
COMMENT ON COLUMN gp_event_reformatted.EVENT_YR IS 'Year of the event/incident/medical episode';


CREATE TABLE gp_event_codes (
    EVENT_CD_ID INTEGER NOT NULL,
    EVENT_CD SMALLINT NOT NULL,
    IS_READ_V2 SMALLINT NOT NULL,
    IS_READ_V3 SMALLINT NOT NULL,
    IS_VALID_CODE SMALLINT NOT NULL,
    DESCRIPTION VARCHAR(255) NOT NULL,
    EVENT_TYPE CHAR(1) NOT NULL,
    HIERARCHY_LEVEL_1 VARCHAR(50) NOT NULL,
    HIERARCHY_LEVEL_1_DESC VARCHAR(255) NOT NULL,
    HIERARCHY_LEVEL_2 VARCHAR(50) NOT NULL,
    HIERARCHY_LEVEL_2_DESC VARCHAR(255) NOT NULL,
    HIERARCHY_LEVEL_3 VARCHAR(50) NOT NULL,
    HIERARHCY_LEVEL_3_DESC VARCHAR(255) NOT NULL
);

COMMENT ON TABLE gp_event_codes IS 'Lookup for event codes and related metadata';
COMMENT ON COLUMN gp_event_codes.EVENT_CD_ID IS 'Links GP_EVENT_REFORMATTED table with GP_EVENT_CODES table';
COMMENT ON COLUMN gp_event_codes.EVENT_CD IS 'Code relating to information recorded during visit';
COMMENT ON COLUMN gp_event_codes.IS_READ_V2 IS '1 if event code is a v2 code, 0 otherwise';
COMMENT ON COLUMN gp_event_codes.IS_READ_V3 IS '1 if event code is a v3 code, 0 otherwise';
COMMENT ON COLUMN gp_event_codes.IS_VALID_CODE IS '1 if code is valid, 0 otherwise';
COMMENT ON COLUMN gp_event_codes.DESCRIPTION IS 'Describes the event taking place during the visit';
COMMENT ON COLUMN gp_event_codes.EVENT_TYPE IS 'Categorises event into medication, event, procedure, and observations (v2 only)';
COMMENT ON COLUMN gp_event_codes.HIERARCHY_LEVEL_1 IS 'Level 1 hierarchy code - only applied to v2 codes';
COMMENT ON COLUMN gp_event_codes.HIERARCHY_LEVEL_1_DESC IS 'Description of level 1 hierarchy code';
COMMENT ON COLUMN gp_event_codes.HIERARCHY_LEVEL_2 IS 'Level 2 hierarchy code - only applies to v2 codes';
COMMENT ON COLUMN gp_event_codes.HIERARCHY_LEVEL_2_DESC IS 'Description of level 2 hierarchy code';
COMMENT ON COLUMN gp_event_codes.HIERARCHY_LEVEL_3 IS 'Level 3 hierarchy code - only applies to v2 codes';
COMMENT ON COLUMN gp_event_codes.HIERARHCY_LEVEL_3_DESC IS 'Description of level 3 hierarchy code';



-- Grant privileges
GRANT ALL ON TABLE SAIL.gp_event_reformatted TO PUBLIC;
GRANT ALL ON TABLE SAIL.gp_event_codes TO PUBLIC;

CONNECT RESET;