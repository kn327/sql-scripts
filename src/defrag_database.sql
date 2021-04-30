If Object_ID('dbo.defrag_database') Is Not Null
	Drop Procedure dbo.defrag_database;
Go

Create Procedure dbo.defrag_database
	@MaxTryCount int = 5
	, @MinFragmentation int = 30
As
	Set Transaction Isolation Level Read Uncommitted;

	Declare
		@object_id int
		, @object_name nvarchar(500)
		, @index_id int
		, @index_name nvarchar(500)
		, @partition_number int

	Declare	IndexCursor Cursor Static Local For
	Select
		i.object_id
		, QuoteName(s.name) + '.' + QuoteName(t.name) As object_name
		, i.index_id
		, QuoteName(i.name) As index_name
		-- if there is no partition scheme, we don't need the partition number
		, Case When ps.function_id Is Not Null Then p.partition_number End As partition_number
	From
		sys.indexes i
		Inner Join sys.tables t
			On t.object_id = i.object_id
		Inner Join sys.schemas s
			On s.schema_id = t.schema_id
		Inner Join sys.partitions p
			On p.object_id = i.object_id And p.index_id = i.index_id
		Left Join sys.partition_schemes ps
			On ps.data_space_id = i.data_space_id
	Where
		i.index_id > 0
		And i.is_disabled = 0
		And s.principal_id = 1

	Open IndexCursor;

	Fetch Next From IndexCursor
	Into @object_id, @object_name, @index_id, @index_name, @partition_number;

	While @@Fetch_Status = 0 Begin
		Declare
			@Try int = 1
			, @Sql nvarchar(max) = N''
			, @Fragmentation float
			, @FragmentationStr varchar(100)
			, @LastFragmentation float = -1
			, @ForceRebuild bit = 0
			, @UpdatedStats bit = 0

		While @Try <= @MaxTryCount Begin
			-- Find fragmentation level
			Select Top 1
				@Fragmentation = ips.avg_fragmentation_in_percent
			From
				sys.dm_db_index_physical_stats(Db_Id(), @object_id, @index_id, @partition_number, 'LIMITED') ips

			Set @FragmentationStr = Convert(varchar(100), @Fragmentation);

			If @Fragmentation <= 5 -- will not waste server power defragmenting 5%
				Break;

			If @Fragmentation <= @MinFragmentation
				Break;

			If @Try = 1 Begin
				If @partition_number Is Not Null
					RaisError('%s.%s (Partition %d)', 10, 15, @object_name, @index_name, @partition_number) With NoWait;
				Else
					RaisError('%s.%s', 10, 15, @object_name, @index_name) With NoWait;
			End

			RaisError('	- Attempt %d => Fragmentation %s', 10, 15, @Try, @FragmentationStr) With NoWait;

			If @Fragmentation = @LastFragmentation Begin
				If @UpdatedStats = 0 Begin
					Set @Sql = 'Update Statistics ' + @object_name + ' ' + @index_name + ';';

					RaisError('		> %s', 10, 15, @Sql) With NoWait;

					Exec sp_executesql @Sql

					Set @UpdatedStats = 1;
					Continue;
				End
				If @ForceRebuild = 0 And @Fragmentation <= 30
					Set @ForceRebuild = 1
				Else If @ForceRebuild = 1 Or @Fragmentation > 30
					Break;
			End

			If @Fragmentation <= 30 And @ForceRebuild = 0 Begin -- Reorganize the index
				Set @Sql = 'Alter Index ' + @index_name + ' On ' + @object_name + ' Reorganize';
				If @partition_number Is Not Null
					Set @Sql = @Sql + ' Partition = ' + Convert(varchar, @partition_number);
			End
			Else Begin -- Rebuild the index
				Set @Sql = 'Alter Index ' + @index_name + ' On ' + @object_name + ' Rebuild'

				If @partition_number Is Not Null
					Set @Sql = @Sql + ' Partition = ' + Convert(varchar, @partition_number);

				Set @Sql = @Sql + ' With (Sort_In_TempDb = On)';
			End

			Set @Sql = @Sql + ';'

			RaisError('		> %s', 10, 15, @Sql) With NoWait;

			Exec sp_executesql @Sql

			Set @UpdatedStats = 0;
			Set @LastFragmentation = @Fragmentation;

			Set @Try += 1;
		End

		If @Try > 1 Begin
			Set @Try -= 1;

			RaisError('	>> Total Attempts %d, Final Fragmentation %s <<', 10, 15, @Try, @FragmentationStr) With NoWait;
		End

		--WaitFor Delay '00:00:01';

		Fetch Next From IndexCursor
		Into @object_id, @object_name, @index_id, @index_name, @partition_number;
	End

	Close IndexCursor;
	Deallocate IndexCursor;

Return 0;
Go
