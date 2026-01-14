-- Migration Script for mssql
ALTER TABLE [transactions] ALTER COLUMN [id] INT NOT NULL PRIMARY KEY CLUSTERED NULL;

ALTER TABLE [transactions] ALTER COLUMN [amount] DECIMAL(10,2) NULL;