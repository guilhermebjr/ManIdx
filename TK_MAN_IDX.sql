/*
Autor: Guilherme Buzana Junior
Data: 02/06/2017
Vers�o: 0.0.7
Procedure MAN_IDX.

Especifica��es :

@BASE Especifica se a manuten��o ocorrer� para todas as bases de dados ou uma base de dados espec�fica.

@DIF_MIN Janela de manuten��o em minutos a partir do momento de execu��o da procedure.
Definir a quantidade de minutos que a procedure deve permanecer em execu��o definindo 
assim sua janela de manuten��o.

@MED_FRAG Percentual de fragmenta��o que o �ndice deve ter para ter manuten��o.
Qualquer �ndice que tiver fragmenta��o igual ou superior a especificada receber� manuten��o.

@TIP_MAN Tipo de manuten��o dos �ndices, caso o valor seja 0 = REORGANIZA, 1 = REBUILD

---- Requisitos.
-- Tabela para registro dos objetos que tiveram manuten��o.
use master
go
create table SER_IDX_HIST(
Db_name varchar(200),
Tb_name varchar(200),
Idx_name varchar(200),
Frag int,
Data datetime
)
go
create table SER_IDX_ERRORLOG(
error_number int,
Data datetime,
error_message varchar(600))

-- An�lise dos dados retornados.
select * from SER_IDX_HIST order by [data] desc
-- Execu��o.
exec MAN_IDX @BASE='todas',@DIF_MIN=60,@MED_FRAG=25,@TIP_MAN=1,@DOP=0,@STATS=1
----------------------------------------------------------------------
ChangeLog
0.0.3
Implementado Rebuild de �ndices na rotina.
@TIP_MAN=1 onde 0 = REORGANIZA, 1 = REBUILD dos �ndices.
Implementado registro de menssagens de erro.
0.0.4
Implementado MAXDOP.
User @dop=0 para autom�tico ou definir quantos processadores usar.
0.0.5
Implementado @STATS=0
Se parametrizado =1 atualiza as estat�sticas ligadas ao �ndice em quest�o.
Padr�o SQL Server =0
0.0.6
Aumentado comprimento dos campos de nome de objetos e comando.
0.0.7 
Corrigido especifica��o de schemas.

*/
use master
go
if exists (select name from sys.objects where name like 'MAN_IDX') drop procedure MAN_IDX
go
create procedure MAN_IDX @BASE varchar(100),@DIF_MIN INT,@MED_FRAG INT,@TIP_MAN int,@DOP smallint,@STATS smallint
with encryption
as 
begin
if exists (select name from tempdb.sys.objects where name like '##table_idx')drop table ##table_idx

set nocount on

DECLARE @TABLE_NAME VARCHAR(500),
@STATS_NAME VARCHAR(500),
@COMANDO NVARCHAR(800),
@HORA_INICIO DATETIME,
@DATABASE_ID int,
@DATABASE_NAME varchar(200),
@IDX_NAME varchar(500),
@TIP_MAN_TEXT varchar(200),
@Frag int,
@STATS_DESC nchar(3)


if @STATS = 1
begin
set @STATS_DESC = 'ON'
end
if @STATS = 0
begin
set @STATS_DESC = 'OFF'
end



--@MED_FRAG INT,
--@DIF_MIN INT

-- Definindo janela de manuten��o
select  @HORA_INICIO= GETDATE()
-- Define comprimento da janela.
--select @DIF_MIN=4
--select @MED_FRAG =10


if @TIP_MAN = 0
set @TIP_MAN_TEXT=' REORGANIZE'
if @TIP_MAN = 1
set @TIP_MAN_TEXT=' REBUILD WITH (MAXDOP='+convert(varchar(2),@DOP)+',STATISTICS_NORECOMPUTE='+@STATS_DESC+')'


-------------------------------- Manuten��o pata todas as bases de dados ------------------------
IF @BASE LIKE 'TODAS'
    BEGIn

DECLARE man_cursor CURSOR FOR 
  SELECT database_id, 
         '[' + name + ']' 
  FROM   sys.databases 
  WHERE  name <> 'master' 
         and name <> 'model' 
         and name <> 'msdb' 
         and name not like'reportserver%' 
         and name <> 'tempdb' 
		 and state=0 and is_read_only=0
  ORDER  BY name 
  
OPEN man_cursor; 

FETCH next FROM man_cursor INTO @DATABASE_ID, @DATABASE_NAME 

WHILE @@FETCH_STATUS = 0 
  BEGIN 
--------------------------------- In�cio do tratamento de �ndices
SELECT @COMANDO = N'SELECT sche.name+''.''+''[''+obj.name+'']'' tb_name,''[''+b.name+'']'' idx_name,a.avg_fragmentation_in_percent
into ##table_idx FROM '+@DATABASE_NAME+'.sys.dm_db_index_physical_stats (' 
+ CONVERT(VARCHAR(4), @DATABASE_ID) 
+ ', NULL, NULL, NULL, NULL) AS a
JOIN '+@DATABASE_NAME+'.sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id 
join '+@DATABASE_NAME+'.sys.objects as obj on a.object_id=obj.object_id
join '+@DATABASE_NAME+'.sys.schemas sche on obj.schema_id=sche.schema_id
where b.name is not null
order by avg_fragmentation_in_percent desc option (maxdop '+convert(varchar(2),@DOP)+');' 
exec sp_executesql @COMANDO
--select * from ##table_idx


DECLARE MAN_IDX CURSOR FOR 
select tb_name,idx_name,avg_fragmentation_in_percent from ##table_idx where idx_name is not null and avg_fragmentation_in_percent >= @MED_FRAG
order by avg_fragmentation_in_percent desc

OPEN MAN_IDX 

FETCH NEXT FROM MAN_IDX INTO @TABLE_NAME,@IDX_NAME,@Frag

WHILE @@FETCH_STATUS = 0 
  BEGIN 
	-- Controle de execu��o da rotina em hor�rio pr�prio
	IF DATEDIFF(MINUTE,@HORA_INICIO,GETDATE()) <= @DIF_MIN
	BEGIN
	begin try
		SET @COMANDO =NULL
        select @COMANDO = N'alter index '+@IDX_NAME+' on '+@DATABASE_NAME+'.'+@TABLE_NAME+@TIP_MAN_TEXT
        EXEC sp_executesql @COMANDO
        INSERT INTO master..SER_IDX_HIST VALUES (@DATABASE_NAME,@TABLE_NAME,@IDX_NAME,@Frag,GETDATE())
	end try
    begin catch
        insert into master.dbo.SER_IDX_ERRORLOG SELECT ERROR_NUMBER() AS ErrorNumber,GETDATE(),ERROR_MESSAGE() AS ErrorMessage;
    end catch
	END
FETCH NEXT FROM MAN_IDX INTO @TABLE_NAME,@IDX_NAME,@Frag
END 

CLOSE MAN_IDX 
DEALLOCATE MAN_IDX 
DROP TABLE ##table_idx

FETCH NEXT FROM man_cursor INTO @DATABASE_ID, @DATABASE_NAME 
END 

CLOSE man_cursor 
DEALLOCATE man_cursor 

set nocount off
end
-- Fim do IF para @BASE='TODAS'
-------------------------------- Manuten��o em base de dados espec�fica ------------------------
  IF @BASE <> 'todas'
    BEGIN
        SET @DATABASE_NAME=@BASE
          IF EXISTS (SELECT Lower(name)
                   FROM   sys.databases
                   WHERE  name LIKE @DATABASE_NAME
				    and state=0 and is_read_only=0)
          BEGIN
          --select @DATABASE_NAME AS 'DATABASE'
              
DECLARE man_cursor CURSOR FOR 
  SELECT database_id, 
         '[' + name + ']' 
  FROM   sys.databases 
  WHERE  name like @BASE

OPEN man_cursor; 

FETCH next FROM man_cursor INTO @DATABASE_ID, @DATABASE_NAME 

WHILE @@FETCH_STATUS = 0 
  BEGIN 
--------------------------------- In�cio do tratamento de �ndices
SELECT @COMANDO = N'SELECT sche.name+''.''+''[''+obj.name+'']'' tb_name,''[''+b.name+'']'' idx_name,a.avg_fragmentation_in_percent
into ##table_idx FROM '+@DATABASE_NAME+'.sys.dm_db_index_physical_stats (' 
+ CONVERT(VARCHAR(4), @DATABASE_ID) 
+ ', NULL, NULL, NULL, NULL) AS a
JOIN '+@DATABASE_NAME+'.sys.indexes AS b ON a.object_id = b.object_id AND a.index_id = b.index_id 
join '+@DATABASE_NAME+'.sys.objects as obj on a.object_id=obj.object_id
join '+@DATABASE_NAME+'.sys.schemas sche on obj.schema_id=sche.schema_id
where b.name is not null
order by avg_fragmentation_in_percent desc option (maxdop '+convert(varchar(2),@DOP)+');' 
exec sp_executesql @COMANDO
--select * from ##table_idx

DECLARE MAN_IDX CURSOR FOR 
select tb_name,idx_name,avg_fragmentation_in_percent from ##table_idx where idx_name is not null and avg_fragmentation_in_percent >= @MED_FRAG
order by avg_fragmentation_in_percent desc

OPEN MAN_IDX 

FETCH NEXT FROM MAN_IDX INTO @TABLE_NAME,@IDX_NAME,@Frag

WHILE @@FETCH_STATUS = 0 
  BEGIN 
	-- Controle de execu��o da rotina em hor�rio pr�prio
	IF DATEDIFF(MINUTE,@HORA_INICIO,GETDATE()) <= @DIF_MIN
	BEGIN
	begin try
		SET @COMANDO =NULL
        select @COMANDO = N'alter index '+@IDX_NAME+' on '+@DATABASE_NAME+'.'+@TABLE_NAME+@TIP_MAN_TEXT
        EXEC sp_executesql @COMANDO
        INSERT INTO master..SER_IDX_HIST VALUES (@DATABASE_NAME,@TABLE_NAME,@IDX_NAME,@Frag,GETDATE())
	end try
    begin catch
        insert into master.dbo.SER_IDX_ERRORLOG SELECT ERROR_NUMBER() AS ErrorNumber,GETDATE(),ERROR_MESSAGE() AS ErrorMessage;
    end catch
	END
FETCH NEXT FROM MAN_IDX INTO @TABLE_NAME,@IDX_NAME,@Frag
END 

CLOSE MAN_IDX 
DEALLOCATE MAN_IDX 
DROP TABLE ##table_idx

FETCH NEXT FROM man_cursor INTO @DATABASE_ID, @DATABASE_NAME 
END 

CLOSE man_cursor 
DEALLOCATE man_cursor 

set nocount off

end

  ELSE
          BEGIN
              PRINT 'Base de dados ou op��o n�o existe ! 
Use @BASE=''todas'' para manuten��o em todas as bases de dados ou @BASE=''nomebase'' para uma base espec�fica.'
          END
-- Fim da 
end
-- End fim da procedure
end