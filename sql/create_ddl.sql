-- diabetes.diabetes_data definition

-- Drop table

-- DROP TABLE diabetes.diabetes_data;

CREATE TABLE diabetes.diabetes_data (
	datetime timestamp NOT NULL,
	glucose float8 NOT NULL,
	carbs float8 NOT NULL,
	bolus float8 NOT NULL,
	basal float8 NOT NULL,
	"hour" int4 NULL,
	hour_group int8 NULL,
	CONSTRAINT diabetes_data_pk PRIMARY KEY (datetime, glucose, carbs, bolus, basal)
);
