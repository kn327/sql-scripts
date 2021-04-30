If Object_ID('dbo.ring_buffer') Is Not Null
	Drop Procedure dbo.ring_buffer;
Go

--Exec dbo.ring_buffer 'xml_deadlock_report'

Create Procedure dbo.ring_buffer
	@event_name varchar(max) = ''
--With Encryption -- uncomment me if you want to encrypt this sproc
As
	-- Resolve the event_name
	Set @event_name = Case @event_name
		When '0' Then 'help'
		When '1' Then 'xml_deadlock_report'
		When '2' Then 'connectivity_ring_buffer_recorded'
		When '3' Then 'scheduler_monitor_system_health_ring_buffer_recorded'
		When '4' Then 'security_error_ring_buffer_recorded'
		When '5' Then 'sp_server_diagnostics_component_result'
		When '6' Then 'memory_broker_ring_buffer_recorded'
		When '7' Then 'wait_info'
		When '8' Then 'error_reported'
		Else @event_name
	End

	If @event_name = 'help' Begin
		Declare
			@text nvarchar(4000)
		Declare
			@Line char(2) = Char(13) + Char(10)

		Set @text = N'Synopsis:' + @Line +
					N'What is in my ring buffer?'  + @Line +
					N'-------------------------------------------------------------------------------------------------------------------------------------'  + @Line +
					N'Description:'  + @Line +
					N'A stored procedure designed to parse and show what is happening in the ring buffer. When specifying a specific event type, the results' + @Line +
					N'will include all extended properties of the event in the appropriate data type.' + @Line +
					N'-------------------------------------------------------------------------------------------------------------------------------------' + @Line +
					N'Parameters:'  + @Line +
					N'ring_buffer null															- all events;' + @Line +
					N'ring_buffer 1 or ''xml_deadlock_report''									- deadlock events;'+ @Line +
					N'ring_buffer 2 or ''connectivity_ring_buffer_recorded''					- connectivity events;'+ @Line +
					N'ring_buffer 3 or ''scheduler_monitor_system_health_ring_buffer_recorded''	- system help events;'+ @Line +
					N'ring_buffer 4 or ''security_error_ring_buffer_recorded''					- security error events;' + @Line +
					N'ring_buffer 5 or ''sp_server_diagnostics_component_result''				- diagnostics component health events;'+ @Line +
					N'ring_buffer 6 or ''memory_broker_ring_buffer_recorded''					- memory broker events;'+ @Line +
					N'ring_buffer 7 or ''wait_info''											- wait events; and,'+ @Line +
					N'ring_buffer 8 or ''error_reported''										- error reported events.'

		Print @text;

		Return 0;
	End

	Set Transaction Isolation Level Read Uncommitted;

	Declare	@XML xml

	Select
		@XML = Cast(t.target_data As xml)-- As target_data
	From
		sys.dm_xe_session_targets t
		Inner Join sys.dm_xe_sessions s
			On s.address = t.event_session_address
	Where
		s.name = 'system_health'
		And t.target_name = 'ring_buffer'

	If Object_ID('tempdb..#records') Is Not Null
		Drop Table #records;

	Create Table #records (
		[timestamp] datetime Not Null
		, name varchar(max) Not Null
		, [record] xml Not Null
	);

	Declare	@Sql nvarchar(max)

	Declare @ProductVersion nvarchar(128) = Convert(nvarchar(128), ServerProperty('ProductVersion'))

	Declare	@ProductVersionMajor int = ParseName(@ProductVersion, 4)
			, @ProductVersionMinor int = ParseName(@ProductVersion, 3)
			, @ProductVersionBuild int = ParseName(@ProductVersion, 2)
	/*
		When trying to parse the deadlock XML graph in earlier versions of SQL Server,
		there is a bug with the XML generated causing invalid XML. To get around this,
		we need to do a quick replacement to fix the XML.
	*/

	Set @Sql = '
	Insert #records ( [timestamp], name, [record] )
	Select
		record.value(''(./@timestamp)[1]'', ''datetime'') As [timestamp]
		, record.value(''(./@name)[1]'', ''varchar(max)'') As name
		, Case
			When record.value(''(./@name)[1]'', ''varchar(max)'') = ''xml_deadlock_report'' And record.value(''(./data/type/@name)[1]'', ''varchar(max)'') = ''xml''
				Then record.query(''(./data/value/*)'')
			When record.value(''(./@name)[1]'', ''varchar(max)'') = ''xml_deadlock_report''
				Then Cast(' + Case
					When @ProductVersionMajor = 10 And @ProductVersionMinor = 0 And @ProductVersionBuild < 2757 Then 'Replace(Replace(record.value(''(./data/value)[1]'', ''nvarchar(max)'')
					, N''<victim-list>'', N''<deadlock><victim-list>'')
							, N''<process-list>'', N''</victim-list><process-list>'')'
					Else 'record.value(''(./data/value)[1]'', ''nvarchar(max)'')'
				End + ' As xml)
			Else record.query(''(./*)'')
		End As record
	From
		@Xml.nodes(''RingBufferTarget/event' + Case When @event_name > '' Then '[@name="' + @event_name + '"]' Else '' End + ''') As x(record)';

	Exec sp_executesql @Sql,
		N'@Xml xml'
		, @Xml = @Xml

	-- try to read from system_health file instead
	If @event_name > '' And (Select Count(*) From #records) = 0 Begin
		Set @Sql = '
		with system_health(name, event_data) As (
			Select
				object_name as name,
				Convert(xml, event_data) as event_data
			From
				sys.fn_xe_file_target_read_file(N''system_health*.xel'', NULL, NULL, NULL)' + Case When @event_name > '' Then '
			Where
				object_name = @event_name' Else '' End + '
		)
		Insert #records ( [timestamp], name, [record] )
		Select
			event_data.value(''(event/@timestamp)[1]'', ''datetime2(7)'') as [timestamp],
			name,
			Case name 
				When ''xml_deadlock_report'' Then event_data.query(''event/data/value/deadlock'')
				Else event_data.query(''(./*)'')
			End As [record]
		From
			system_health'

		Exec sp_executesql @Sql,
			N'@event_name varchar(max)',
			@event_name = @event_name;
	End

	If @event_name = 'xml_deadlock_report'
		Select
			r.[timestamp]
			, Case
				When vl.record.value('(./@id)[1]', 'nvarchar(max)') = pl.record.value('(./@id)[1]', 'nvarchar(max)') Then 1
				Else 0
			End As [is_victim]
			, pl.record.value('(./@id)[1]', 'nvarchar(max)') As [process_id]
			, Case
				When pl.record.value('(./inputbuf)[1]', 'varchar(max)') = Char(10) + 'Proc [Database Id = ' + Convert(varchar(max), st.dbid) + ' Object Id = ' + Convert(varchar(max), st.objectid)+ ']   ' Then 1
				Else 0
			End As [is_root]

			--, st.dbid As [database_id]
			--, st.objectid As [object_id]

			, Db_Name(st.dbid) As [database_name]
			, Object_Schema_Name(st.objectid, st.dbid) As [schema_name]
			, Object_Name(st.objectid, st.dbid) As [object_name]
			--, es.record.value('(./@line)[1]', 'int') As [line]
			, Case
				When st.objectid Is Null Then pl.record.value('(./inputbuf)[1]', 'nvarchar(max)')
				Else SubString(st.text,
					(es.record.value('(./@stmtstart)[1]', 'int') / 2) + 1,
					((Case es.record.value('(./@stmtend)[1]', 'int')
						When -1 Then DataLength(st.text)
						Else es.record.value('(./@stmtend)[1]', 'int')
					End - es.record.value('(./@stmtstart)[1]', 'int')) / 2) + 1)
			End As [statement_text]

			--, pl.record.value('(./inputbuf)[1]', 'nvarchar(max)')
			--, es.record.value('(./@sqlhandle)[1]', 'varchar(max)') As [sql_handle]

			, pl.record.value('(./@taskpriority)[1]', 'int') As [task_priority]
			, pl.record.value('(./@logused)[1]', 'int') As [log_used]
			, pl.record.value('(./@waitresource)[1]', 'nvarchar(max)') As [wait_resource]
			, pl.record.value('(./@waittime)[1]', 'int') As [wait_time]
			, pl.record.value('(./@ownerId)[1]', 'bigint') As [owner_id]
			, pl.record.value('(./@transactionname)[1]', 'nvarchar(max)') As [transaction_name]
			, pl.record.value('(./@lasttranstarted)[1]', 'datetime') As [last_tran_started]
			, pl.record.value('(./@XDES)[1]', 'nvarchar(max)') As [xdes]
			, pl.record.value('(./@lockMode)[1]', 'nvarchar(max)') As [lock_mode]
			, pl.record.value('(./@schedulerid)[1]', 'int') As [scheduler_id]
			, pl.record.value('(./@kpid)[1]', 'int') As [kpid]
			, pl.record.value('(./@status)[1]', 'nvarchar(max)') As [status]
			, pl.record.value('(./@spid)[1]', 'int') As [spid]
			, pl.record.value('(./@sbid)[1]', 'int') As [sbid]
			, pl.record.value('(./@ecid)[1]', 'int') As [ecid]
			, pl.record.value('(./@priority)[1]', 'int') As [priority]
			, pl.record.value('(./@trancount)[1]', 'int') As [tran_count]
			, pl.record.value('(./@lastbatchstarted)[1]', 'datetime') As [last_batch_started]
			, pl.record.value('(./@lastbatchcompleted)[1]', 'datetime') As [last_batch_completed]
			, pl.record.value('(./@clientapp)[1]', 'nvarchar(max)') As [client_app]
			, pl.record.value('(./@hostname)[1]', 'nvarchar(max)') As [host_name]
			, pl.record.value('(./@hostpid)[1]', 'int') As [host_pid]
			, pl.record.value('(./@loginname)[1]', 'nvarchar(max)') As [login_name]
			, pl.record.value('(./@isolationlevel)[1]', 'nvarchar(max)') As [isolation_level]
			, pl.record.value('(./@xactid)[1]', 'bigint') As [xact_id]
			, pl.record.value('(./@currentdb)[1]', 'int') As [current_db]
			, pl.record.value('(./@lockTimeout)[1]', 'bigint') As [lock_timeout]
			, pl.record.value('(./@clientoption1)[1]', 'bigint') As [client_option_1]
			, pl.record.value('(./@clientoption2)[1]', 'bigint') As [client_option_2]
		From
			#records r
			Cross Apply r.record.nodes('deadlock/victim-list/victimProcess') As vl(record)
			Cross Apply r.record.nodes('deadlock/process-list/process') As pl(record)
			Cross Apply pl.record.nodes('executionStack/frame') As es(record)
			Cross Apply sys.dm_exec_sql_text(Convert(varbinary(64), es.record.value('(./@sqlhandle)[1]', 'varchar(max)'), 1)) As st
		Order By
			[timestamp] Desc
			, is_victim-- Desc
			, process_id
			, es.record.value('(./@line)[1]', 'int')
	Else If @event_name = 'connectivity_ring_buffer_recorded'
		Select
			r.[timestamp]
			--, r.record.value('(/data[@name="id"]/value)[1]', 'bigint') As record_id -- appears to be unused

			, r.record.value('(/data[@name="type"]/value)[1]', 'int') As [type] -- connectivity_record_type
			, r.record.value('(/data[@name="type"]/text)[1]', 'varchar(max)') As type_desc -- connectivity_record_type_desc

			, r.record.value('(/data[@name="source"]/value)[1]', 'int') As [source] -- connectivity_record_source
			, r.record.value('(/data[@name="source"]/text)[1]', 'varchar(max)') As source_desc -- connectivity_record_source_desc

			, r.record.value('(/data[@name="session_id"]/value)[1]', 'int') As session_id
			, r.record.value('(/data[@name="os_error"]/value)[1]', 'bigint') As os_error

			, r.record.value('(/data[@name="sni_consumer_error"]/value)[1]', 'bigint') As sni_consumer_error
			, r.record.value('(/data[@name="sni_provider"]/value)[1]', 'int') As sni_provider

			, r.record.value('(/data[@name="state"]/value)[1]', 'bigint') As [state]
			, r.record.value('(/data[@name="local_port"]/value)[1]', 'int') As local_port
			, r.record.value('(/data[@name="remote_port"]/value)[1]', 'int') As remote_port

			, r.record.value('(/data[@name="tds_input_buffer_error"]/value)[1]', 'bigint') As tds_input_buffer_error
			, r.record.value('(/data[@name="tds_output_buffer_error"]/value)[1]', 'bigint') As tds_output_buffer_error
			, r.record.value('(/data[@name="tds_input_buffer_bytes"]/value)[1]', 'bigint') As tds_input_buffer_bytes

			, r.record.value('(/data[@name="tds_flags"]/value)[1]', 'varchar(max)') As tds_flags -- connectivity_record_tds_flag
			, r.record.value('(/data[@name="tds_flags"]/text)[1]', 'varchar(max)') As tds_flags_desc -- connectivity_record_tds_flag_desc

			, r.record.value('(/data[@name="total_login_time_ms"]/value)[1]', 'bigint') As total_login_time_ms
			, r.record.value('(/data[@name="login_task_enqueued_ms"]/value)[1]', 'bigint') As login_task_enqueued_ms
			, r.record.value('(/data[@name="network_writes_ms"]/value)[1]', 'bigint') As network_writes_ms
			, r.record.value('(/data[@name="network_reads_ms"]/value)[1]', 'bigint') As network_reads_ms
			, r.record.value('(/data[@name="ssl_processing_ms"]/value)[1]', 'bigint') As ssl_processing_ms
			, r.record.value('(/data[@name="sspi_processing_ms"]/value)[1]', 'bigint') As sspi_processing_ms
			, r.record.value('(/data[@name="login_trigger_and_resource_governor_processing_ms"]/value)[1]', 'bigint') As login_trigger_and_resource_governor_processing_ms

			, r.record.value('(/data[@name="connection_id"]/value)[1]', 'uniqueidentifier') As connection_id
			, r.record.value('(/data[@name="connection_peer_id"]/value)[1]', 'uniqueidentifier') As connection_peer_id
			, r.record.value('(/data[@name="local_host"]/value)[1]', 'varchar(max)') As local_host
			, r.record.value('(/data[@name="remote_host"]/value)[1]', 'varchar(max)') As remote_host

			, r.record.value('(/data[@name="call_stack"]/value)[1]', 'varchar(max)') As call_stack
		From
			#records r
		Order By
			[timestamp] Desc
	Else If @event_name = 'error_reported'
		Select
			r.[timestamp]
			--, r.record.value('(/data[@name="id"]/value)[1]', 'bigint') As record_id -- appears to be unused

			, r.record.value('(/data[@name="error"]/value)[1]', 'int') As error
			, r.record.value('(/data[@name="severity"]/value)[1]', 'int') As severity
			, r.record.value('(/data[@name="state"]/value)[1]', 'int') As [state]
			, r.record.value('(/data[@name="user_defined"]/value)[1]', 'bit') As user_defined
			, r.record.value('(/data[@name="message"]/value)[1]', 'varchar(max)') As [message]

			, r.record.value('(/action[@name="callstack"]/value)[1]', 'varchar(max)') As callstack
			, r.record.value('(/action[@name="session_id"]/value)[1]', 'int') As session_id
			, r.record.value('(/action[@name="sql_text"]/value)[1]', 'varchar(max)') As sql_text
			, r.record.value('(/action[@name="tsql_stack"]/value)[1]', 'varchar(max)') As tsql_stack
		From
			#records r
		Order By
			r.[timestamp] Desc
	Else If @event_name = 'scheduler_monitor_system_health_ring_buffer_recorded'
		Select
			r.[timestamp]
			--, r.record.value('(/data[@name="id"]/value)[1]', 'bigint') As record_id -- appears to be unused

			, r.record.value('(/data[@name="process_utilization"]/value)[1]', 'bigint') As process_utilization
			, r.record.value('(/data[@name="system_idle"]/value)[1]', 'bigint') As system_idle
			, r.record.value('(/data[@name="user_mode_time"]/value)[1]', 'bigint') As user_mode_time
			, r.record.value('(/data[@name="kernel_mode_time"]/value)[1]', 'bigint') As kernel_mode_time
			, r.record.value('(/data[@name="page_faults"]/value)[1]', 'bigint') As page_faults
			, r.record.value('(/data[@name="working_set_delta"]/value)[1]', 'bigint') As working_set_delta
			, r.record.value('(/data[@name="memory_utilization"]/value)[1]', 'bigint') As memory_utilization
			--, r.record.value('(/data[@name="call_stack"]/value)[1]', 'varchar(max)') As call_stack -- appears to be unused
		From
			#records r
		Order By
			r.[timestamp] Desc
	Else If @event_name = 'security_error_ring_buffer_recorded'
		Select
			r.[timestamp]
			--, r.record.value('(/data[@name="id"]/value)[1]', 'bigint') As record_id -- appears to be unused

			, r.record.value('(/data[@name="session_id"]/value)[1]', 'int') As session_id

			, r.record.value('(/data[@name="error_code"]/value)[1]', 'bigint') As error_code

			, r.record.value('(/data[@name="api_name"]/value)[1]', 'varchar(max)') As api_name
			, r.record.value('(/data[@name="calling_api_name"]/value)[1]', 'varchar(max)') As calling_api_name

			, r.record.value('(/data[@name="call_stack"]/value)[1]', 'varchar(max)') As call_stack
		From
			#records r
		Order By
			[timestamp] Desc
	Else If @event_name = 'sp_server_diagnostics_component_result'
		Select
			r.[timestamp]
			, r.record.value('(/data[@name="component"]/value)[1]', 'int') As component
			, r.record.value('(/data[@name="component"]/text)[1]', 'varchar(max)') As component_desc
			, r.record.value('(/data[@name="state"]/value)[1]', 'int') As [state]
			, r.record.value('(/data[@name="state"]/text)[1]', 'varchar(max)') As state_desc

			, r.record.query('(./data[@name="data"]/value/*)') As data
		From
			#records r
		Order By
			[timestamp] Desc
	Else If @event_name = 'memory_broker_ring_buffer_recorded'
		Select
			r.[timestamp]
			, r.record.value('(/data[@name="delta_time"]/value)[1]', 'bigint') As delta_time
			, r.record.value('(/data[@name="memory_ratio"]/value)[1]', 'bigint') As memory_ratio
			, r.record.value('(/data[@name="new_target"]/value)[1]', 'bigint') As new_target
			, r.record.value('(/data[@name="overall"]/value)[1]', 'bigint') As overall
			, r.record.value('(/data[@name="rate"]/value)[1]', 'bigint') As rate
			, r.record.value('(/data[@name="currently_predicated"]/value)[1]', 'bigint') As currently_predicated
			, r.record.value('(/data[@name="currently_allocated"]/value)[1]', 'bigint') As currently_allocated
			, r.record.value('(/data[@name="previously_allocated"]/value)[1]', 'bigint') As previously_allocated
			, r.record.value('(/data[@name="broker"]/value)[1]', 'varchar(max)') As [broker]
			, r.record.value('(/data[@name="notification"]/value)[1]', 'varchar(max)') As [notification]

			, r.record.value('(/data[@name="call_stack"]/value)[1]', 'varchar(max)') As call_stack
		From
			#records r
		Order By
			[timestamp] Desc
	Else If @event_name = 'wait_info'
		Select
			r.[timestamp]
			, r.record.value('(/data[@name="wait_type"]/value)[1]', 'int') As wait_type
			, r.record.value('(/data[@name="wait_type"]/text)[1]', 'varchar(max)') As wait_type_desc

			, r.record.value('(/data[@name="opcode"]/value)[1]', 'int') As opcode
			, r.record.value('(/data[@name="opcode"]/text)[1]', 'varchar(max)') As opcode_desc

			, r.record.value('(/data[@name="duration"]/value)[1]', 'bigint') As duration
			, r.record.value('(/data[@name="max_duration"]/value)[1]', 'bigint') As [max_duration]
			, r.record.value('(/data[@name="total_duration"]/value)[1]', 'bigint') As total_duration
			, r.record.value('(/data[@name="signal_duration"]/value)[1]', 'bigint') As signal_duration
			, r.record.value('(/data[@name="completed_count"]/value)[1]', 'bigint') As completed_count

			, r.record.value('(/action[@name="callstack"]/value)[1]', 'varchar(max)') As callstack
			, r.record.value('(/action[@name="session_id"]/value)[1]', 'int') As session_id
			, r.record.value('(/action[@name="sql_text"]/value)[1]', 'varchar(max)') As sql_text
		From
			#records r
		Order By
			[timestamp] Desc
	Else
		Select
			[timestamp]
			, name As record_name
			, [record] As data
		From
			#records
		Order By
			[timestamp] Desc
			, record_name

Return 0;
Go
