-- Destructive: drops data. Restore with Time Travel (UNDROP TABLE) within the retention window.
DROP TABLE IF EXISTS {{ DATABASE }}.CORE.ORDERS;
DROP TABLE IF EXISTS {{ DATABASE }}.RAW.ORDERS;
