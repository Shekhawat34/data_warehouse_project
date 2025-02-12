/* 

Create Database and Schemas

Script Puspose:

THis Script creats a new database named "DataWarehouse" after checking if it already exists.
if the Database exists, it is dropped and recreated.Additionally, the script sets up three schemas
within the database  - "Bronze", "Silver", and "Gold".
*/

use master;

go

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
     alter database DataWarehouse set single_user with rollback immediate;
	 drop database DataWarehouse;
end;

go

create database DataWarehouse;

use DataWarehouse;
go

-- created the schemas 

create schema bronze;
go

create schema silver;
go

create schema gold;
go
