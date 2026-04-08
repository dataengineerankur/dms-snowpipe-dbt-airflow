-- SF001 - Warehouse not available
-- Category: warehouse
-- Description: configured virtual warehouse does not exist
-- Fixed: Removed non-existent role and invalid table reference

SELECT CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_USER();
