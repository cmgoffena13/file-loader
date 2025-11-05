-- Create database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'fileloader')
BEGIN
    CREATE DATABASE fileloader;
END
GO

