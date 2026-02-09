export const TABLE_NAME = "river_data";

// Define the schema mapping field names to SQLite data types
export const SCHEMA_MAPPING = {
  id: "TEXT",
  stationName: "TEXT NOT NULL",
  stationType: "TEXT",
  timeDay: "TEXT",
  observedAt: "TEXT NOT NULL", // Unique with stationName
  issuedAt: "TEXT NOT NULL",
  heightM: "REAL",
  gaugeDatum: "TEXT",
  tendency: "TEXT",
  crossingM: "TEXT",
  floodClassification: "TEXT",
  fetchedAt: "TEXT NOT NULL", // Time the row was created
};

/**
 * Creates the river_data table and associated indices.
 * @param {import('node:sqlite').DatabaseSync} dbInstance
 */
export function createRiverData(dbInstance) {
  // 1. Generate the SQL for the table columns
  const columnsSql = Object.entries(SCHEMA_MAPPING)
    .map(([columnName, dataType]) => `${columnName} ${dataType}`)
    .join(", \n  ");

  // 2. Construct and execute the CREATE TABLE statement
  const createTableSql = `
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
  ${columnsSql},
  UNIQUE (stationName, observedAt)
) STRICT;`;

  dbInstance.exec(createTableSql);

  // 3. Add an index for fast querying by station and time
  dbInstance.exec(`
CREATE INDEX IF NOT EXISTS idx_timeseries ON ${TABLE_NAME} (stationName, observedAt);
`);
}
