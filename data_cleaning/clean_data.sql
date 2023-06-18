-------------------------------------------------------
-- replace nulls
select "PropertyAddress" from raw_tbl where "PropertyAddress" is null;

-- same parcel ID has same address
select * from raw_tbl order by "ParcelID" limit 100;
select "ParcelID", lag("ParcelID") over (partition by "ParcelID" order by "ParcelID") as prevPID, "PropertyAddress" from raw_tbl  limit 100;


select count("ParcelID") from raw_tbl where "ParcelID" is null;

select trg."UniqueID ", trg."ParcelID", trg."PropertyAddress", src."ParcelID", src."PropertyAddress", src."UniqueID "
from raw_tbl src inner join raw_tbl trg
on src."ParcelID" = trg."ParcelID"
and src."UniqueID " <> trg."UniqueID "
where trg."PropertyAddress" is null
;

-- insert into null rows based on parcelID and uniqueID
update raw_tbl
set "PropertyAddress" =  ifnull(trg."PropertyAddress",src."PropertyAddress")
from raw_tbl src inner join raw_tbl trg
on src."ParcelID" = trg."ParcelID"
and src."UniqueID " <> trg."UniqueID "
where trg."PropertyAddress" is null
;
select "PropertyAddress" from raw_tbl where "PropertyAddress" is null;
-------------------------------------------------------
--date format
select to_date("SaleDate", 'MMMM DD, YYYY')from raw_tbl limit 10;

select "SaleDate", to_date("SaleDate", 'MMMM DD, YYYY')from raw_tbl limit 30;

-- date is varchar in raw_tbl, so insert new column with DATE type
update raw_tbl set "SaleDate" = to_date("SaleDate", 'MMMM DD, YYYY');

alter table raw_tbl add column saledate2 Date;
update raw_tbl set saledate2 = to_date("SaleDate");
alter table raw_tbl drop column "SaleDate";
alter table raw_tbl rename column saledate2 to "SaleDate";
-------------------------------------------------------
-- separate address

-- 3 ways to separate
select  substr("PropertyAddress",1,charindex(',',"PropertyAddress",1)-1) as charindex_way1
, substr("PropertyAddress",1,position(',',"PropertyAddress",1)-1) as position_way1
, split_part("PropertyAddress", ',', 1) as split_way1
, substr("PropertyAddress",charindex(',',"PropertyAddress",1),charindex(',',"PropertyAddress",1)+1) as charindex_way2
, substr("PropertyAddress",position(',',"PropertyAddress",1),position(',',"PropertyAddress",1)+1) as position_way2
, split_part("PropertyAddress", ',', 2) as split_way2
from raw_tbl limit 10;

alter table raw_tbl
add column "PropertyCity" string;
update raw_tbl
set "PropertyCity" = split_part(src."PropertyAddress", ',', 2) from raw_tbl src
where raw_tbl."UniqueID " = src."UniqueID "
;
update raw_tbl
set "PropertyAddress" = split_part(src."PropertyAddress", ',', 1) from raw_tbl src
where raw_tbl."UniqueID " = src."UniqueID "
;
-- same for owner address
select split_part("OwnerAddress", ',', 1)
, split_part("OwnerAddress", ',', 2)
, split_part("OwnerAddress", ',', 3)
from raw_tbl limit 5;

alter table raw_tbl
add column "OwnerCity" string;
alter table raw_tbl
add column "OwnerState" string;
update raw_tbl
set "OwnerCity" = split_part(src."OwnerAddress", ',', 2) from raw_tbl src
where raw_tbl."UniqueID " = src."UniqueID "
;
update raw_tbl
set "OwnerState" = split_part(src."OwnerAddress", ',', 3) from raw_tbl src
where raw_tbl."UniqueID " = src."UniqueID "
;
update raw_tbl
set "OwnerAddress" = split_part(src."OwnerAddress", ',', 1) from raw_tbl src
where raw_tbl."UniqueID " = src."UniqueID "
;

-------------------------------------------------------
-- remove duplicate rows

select  *
, row_number() over (partition by "ParcelID", "PropertyAddress", "PropertyCity", "SalePrice", "SaleDate", "LegalReference" order by "UniqueID ") as dup_uniq
from raw_tbl;

select count(*) from (
select  *
, row_number() over (partition by "ParcelID", "PropertyAddress", "PropertyCity", "SalePrice", "SaleDate", "LegalReference" order by "UniqueID ") as dup_uniq
from raw_tbl) subq
where subq.dup_uniq <>1;

select "UniqueID ", dup_uniq from (
select  *
, row_number() over (partition by "ParcelID", "PropertyAddress", "PropertyCity", "SalePrice", "SaleDate", "LegalReference" order by "UniqueID ") as dup_uniq
from raw_tbl
) subq where subq.dup_uniq <>1;

delete from raw_tbl using 
(
select  "ParcelID", "PropertyAddress", "PropertyCity", "SalePrice", "SaleDate", "LegalReference" , "UniqueID "
, row_number() over (partition by "ParcelID", "PropertyAddress", "PropertyCity", "SalePrice", "SaleDate", "LegalReference" order by "UniqueID ") as dup_uniq
from raw_tbl
) as subq
where raw_tbl."UniqueID " = subq."UniqueID " and subq.dup_uniq <>1
;

------------------------------------------------------
--renaming to cleaned
alter table raw_tbl
rename to clean_tbl;
