use async_std::io::Result;
use oxidized_mdf::MdfDatabase;
use pretty_assertions::assert_eq;
use rstest::rstest;

#[rstest(
    file,
    db_name,
    case("spg_verein_TST.mdf", "spg_verein_TST"),
    case("AWLT2005.mdf", "AdventureWorksLT")
)]
#[async_std::test]
async fn database_name(file: &str, db_name: &str) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;
    assert_eq!(db.database_name(), db_name);
    Ok(())
}

#[rstest(
    file,
    table_names,
    case("spg_verein_TST.mdf", vec!["TABLE_PRIVILEGES", "dm_db_mirroring_past_actions", "dm_db_stats_properties_internal", "dm_db_xtp_nonclustered_index_stats", "dm_exec_cached_plan_dependent_objects", "dm_os_hosts", "dm_os_memory_allocations", "dm_os_memory_brokers", "dm_pdw_nodes_os_schedulers", "dm_pdw_nodes_os_tasks", "dm_pdw_nodes_resource_governor_workload_groups", "dm_resource_governor_resource_pool_volumes", "dm_xe_session_object_columns", "fn_cdc_get_column_ordinal", "fn_sqlvarbasetostr", "openkeys", "pdw_nodes_partitions", "sp_MSadd_repl_command", "sp_MSaddlightweightmergearticle", "sp_MSaddmergedynamicsnapshotjob", "sp_MSallocate_new_identity_range", "sp_MSalreadyhavegeneration", "sp_MSarticlecleanup", "sp_MSchecksnapshotstatus", "sp_MScomputemergearticlescreationorder", "sp_MScreate_sub_tables", "sp_MSdrop_distribution_agent", "sp_MSdynamicsnapshotjobexistsatdistributor", "sp_MSenum_merge_subscriptions", "sp_MSenum_snapshot", "sp_MSenumpartialchangesdirect", "sp_MSexecwithlsnoutput", "sp_MSget_subscription_dts_info", "sp_MSgetagentoffloadinfo", "sp_MSgetconflicttablename", "sp_MSgetdynamicsnapshotapplock", "sp_MShelp_distribution_agentid", "sp_MShelp_identity_property", "sp_MShelpvalidationdate", "sp_MSremove_mergereplcommand", "sp_MSrepl_agentstatussummary", "sp_MSresetsnapshotdeliveryprogress", "sp_MSstopdistribution_agent", "sp_MSupdate_subscriber_info", "sp_MSwritemergeperfcounter", "sp_addpullsubscription", "sp_addqueued_artinfo", "sp_addserver", "sp_assemblies_rowset_rmt", "sp_autostats", "sp_bindsession", "sp_cdc_disable_table", "sp_changelogreader_agent", "sp_columns_90_rowset_rmt", "sp_cycle_errorlog", "sp_dropmergepullsubscription", "sp_dropremotelogin", "sp_help_spatial_geometry_index_xml", "sp_is_makegeneration_needed", "sp_new_parallel_nested_tran_id", "sp_password", "sp_procedure_params_100_rowset2", "sp_publishdb", "sp_redirect_publisher", "sp_rename", "sp_replcounters", "sp_replicationdboption", "sp_replmonitorrefreshjob", "sp_replsendtoqueue", "sp_replsetsyncstatus", "sp_replshowcmds", "sp_script_reconciliation_sinsproc", "sp_start_user_instance", "xp_instance_regread"]),
    case("AWLT2005.mdf", vec!["Address", "BuildVersion", "CK_Product_ListPrice", "CK_Product_SellEndDate", "CK_Product_StandardCost", "CK_Product_Weight", "CK_SalesOrderDetail_OrderQty", "CK_SalesOrderDetail_UnitPrice", "CK_SalesOrderDetail_UnitPriceDiscount", "Customer", "CustomerAddress", "DF_Address_ModifiedDate", "DF_Address_rowguid", "DF_BuildVersion_ModifiedDate", "DF_CustomerAddress_ModifiedDate", "DF_CustomerAddress_rowguid", "DF_Customer_ModifiedDate", "DF_Customer_NameStyle", "DF_Customer_rowguid", "DF_ProductCategory_ModifiedDate", "DF_ProductCategory_rowguid", "DF_ProductDescription_ModifiedDate", "DF_ProductDescription_rowguid", "DF_ProductModelProductDescription_ModifiedDate", "DF_ProductModelProductDescription_rowguid", "DF_ProductModel_ModifiedDate", "DF_ProductModel_rowguid", "DF_Product_ModifiedDate", "DF_Product_rowguid", "DF_SalesOrderDetail_ModifiedDate", "DF_SalesOrderDetail_UnitPriceDiscount", "DF_SalesOrderDetail_rowguid", "DF_SalesOrderHeader_Freight", "DF_SalesOrderHeader_OnlineOrderFlag", "DF_SalesOrderHeader_OrderDate", "DF_SalesOrderHeader_RevisionNumber", "DF_SalesOrderHeader_Status", "DF_SalesOrderHeader_SubTotal", "DF_SalesOrderHeader_TaxAmt", "DF_SalesOrderHeader_rowguid", "Product", "ProductCategory", "ProductDescription", "ProductModel", "ProductModelProductDescription", "SalesOrderDetail", "SalesOrderHeader", "sysallocunits", "sysasymkeys", "sysbinobjs", "sysbinsubobjs", "syscerts", "sysclsobjs", "syscolpars", "sysconvgroup", "sysdbfiles", "sysdercv", "sysdesend", "sysfiles1", "sysftinds", "sysguidrefs", "syshobtcolumns", "syshobts", "sysidxstats", "sysiscols", "sysmultiobjrefs", "sysnsobjs", "sysobjkeycrypts", "sysobjvalues", "sysowners", "sysprivs", "sysqnames", "sysremsvcbinds", "sysrowsetcolumns", "sysrowsetrefs", "sysrowsets", "sysrts", "sysscalartypes", "sysschobjs", "sysserefs", "syssingleobjrefs", "syssqlguides", "systypedsubobjs", "sysxmitqueue", "sysxmlcomponent", "sysxmlfacet", "sysxmlplacement", "sysxprops"])
)]
#[async_std::test]
async fn tables(file: &str, table_names: Vec<&str>) -> Result<()> {
    let db = MdfDatabase::open(format!("data/{}", file)).await?;

    let mut tables = db.table_names();
    tables.sort();

    assert_eq!(tables, table_names);

    Ok(())
}
