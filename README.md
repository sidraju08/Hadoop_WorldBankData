# Hadoop_WorldBankData
create database if not exists sidpoc;

use sidpoc;


========================================================================================================================================
1. Filter the data based on "Access Option" is "Bulk download"
========================================================================================================================================

drop table worldbank;

create table worldbank(datacatalog_id string, name string, acronym string, description string, url string, type string,	language_supported string, periodicity string, economy_coverage string, granularity string, list_of_countries string, number_of_economies string, data_notes string, topics string, update_frequency string, update_schedule string, last_revision_date string, contact_details string,	access_option string, bulk_download string, api string, attribution_citation string, source_url string, detail_page_url string, coverage string, api_access_url string) row format delimited fields terminated by "\t" lines terminated by "\n" stored as textfile;

load data local inpath '/home/hadoop/work/sid_hadoop_pocs/world_bank_data/world_bank_data_catalog' overwrite into table worldbank;

select * from worldbank;

create view filter_access_option as select * from worldbank where instr(access_option,'Bulk download') > 0;

select * from filter_access_option;


=========================================================================
2. Partition the data based on "Periodicity"
=========================================================================

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table periodicity_wise_partition; 

create table if not exists periodicity_wise_partition(datacatalog_id string, name string, acronym string, description string, url string, type string, language_supported string, economy_coverage string, granularity string, list_of_countries string, number_of_economies string, data_notes string, topics string, update_frequency string, update_schedule string, last_revision_date string, contact_details string,	access_option string, bulk_download string, api string, attribution_citation string, source_url string, detail_page_url string, coverage string, api_access_url string) partitioned by (periodicity string) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

from worldbank
insert overwrite table periodicity_wise_partition partition (periodicity='Month') select datacatalog_id , name , acronym , description , url , type , language_supported , economy_coverage , granularity , list_of_countries , number_of_economies , data_notes , topics , update_frequency , update_schedule , last_revision_date , contact_details ,	access_option , bulk_download , api , attribution_citation , source_url , detail_page_url , coverage , api_access_url where periodicity = 'Month'

insert overwrite table periodicity_wise_partition partition (periodicity='Annual') select datacatalog_id , name , acronym , description , url , type , language_supported , economy_coverage , granularity , list_of_countries , number_of_economies , data_notes , topics , update_frequency , update_schedule , last_revision_date , contact_details ,	access_option , bulk_download , api , attribution_citation , source_url , detail_page_url , coverage , api_access_url where periodicity = 'Annual'

insert overwrite table periodicity_wise_partition partition (periodicity='Day') select datacatalog_id , name , acronym , description , url , type , language_supported , economy_coverage , granularity , list_of_countries , number_of_economies , data_notes , topics , update_frequency , update_schedule , last_revision_date , contact_details ,	access_option , bulk_download , api , attribution_citation , source_url , detail_page_url , coverage , api_access_url where periodicity = 'Day'

insert overwrite table periodicity_wise_partition partition (periodicity='Quarter') select datacatalog_id , name , acronym , description , url , type , language_supported , economy_coverage , granularity , list_of_countries , number_of_economies , data_notes , topics , update_frequency , update_schedule , last_revision_date , contact_details ,	access_option , bulk_download , api , attribution_citation , source_url , detail_page_url , coverage , api_access_url where periodicity = 'Quarter'
;

show partitions periodicity_wise_partition;

select * from periodicity_wise_partition where periodicity = 'Quarter';


=========================================================================
3. Provide the partition data based on coverage between below 1970, 1970 - 2010 and above 2010
=========================================================================

drop table coverage_wise_partition; 

create table if not exists coverage_wise_partition(datacatalog_id string, name string, acronym string, description string, url string, type string,	language_supported string, periodicity string, economy_coverage string, granularity string, list_of_countries string, number_of_economies string, data_notes string, topics string, update_frequency string, update_schedule string, last_revision_date string, contact_details string,	access_option string, bulk_download string, api string, attribution_citation string, source_url string, detail_page_url string, coverage string, api_access_url string) partitioned by (coveragedata string) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

from worldbank
insert overwrite table coverage_wise_partition partition (coveragedata='lessthan1970') select * where isValidCondition(coverage,'1970-')

insert overwrite table coverage_wise_partition partition (coveragedata='between1970to2010') select * where isValidCondition(coverage,'1970-2010')

insert overwrite table coverage_wise_partition partition (coveragedata='above2010') select * where isValidCondition(coverage,'2010+')
;

insert overwrite table coverage_wise_partition partition (coveragedata='lessthan2000') select * from worldbank where isValidCondition(coverage,'2000-');

show partitions coverage_wise_partition;

select coverage from coverage_wise_partition where coveragedata='lessthan1970';
select coverage from coverage_wise_partition where coveragedata='between1970to2010';
select coverage from coverage_wise_partition where coveragedata='above2010';
select coverage from coverage_wise_partition where coveragedata='lessthan2000';



