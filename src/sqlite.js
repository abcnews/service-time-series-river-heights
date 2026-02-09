import { DatabaseSync } from "node:sqlite";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createRiverData,
  SCHEMA_MAPPING,
  TABLE_NAME,
} from "./migrations/00-create-river_data.js";
import logger from "./logger.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_DATABASE_FILE = path.resolve(
  __dirname,
  "../data/rivers.sqlite",
);

let dbInstance = null;

/**
 * Initializes the database and runs migrations.
 * @param {string} [dbPath]
 * @returns {DatabaseSync}
 */
export function initializeDatabase(dbPath = DEFAULT_DATABASE_FILE) {
  if (dbInstance) {
    return dbInstance;
  }

  try {
    dbInstance = new DatabaseSync(dbPath);
    createRiverData(dbInstance);

    logger.info("Database '%s' loaded", dbPath);
    return dbInstance;
  } catch (e) {
    logger.error("Fatal error during database initialization: %s", e.message);
    if (dbInstance) dbInstance.close();
    throw e;
  }
}

/**
 * Appends multiple records to the river_data table.
 * @param {Array<Object>} records
 */
export async function appendRecords(records) {
  const db = initializeDatabase();
  const fetchedAt = new Date().toISOString();

  const columnNames = Object.keys(SCHEMA_MAPPING);
  const placeholders = columnNames.map(() => "?").join(", ");
  const colsListSql = columnNames.join(", ");

  const insertSql = `
INSERT OR IGNORE INTO ${TABLE_NAME} (${colsListSql}) 
VALUES (${placeholders})
`;

  const insertStmt = db.prepare(insertSql);
  let insertCount = 0;

  try {
    for (const record of records) {
      if (!record.stationName || !record.observedAt) {
        logger.warn("Skipping invalid record: missing stationName or observedAt");
        continue;
      }

      const data = {
        ...record,
        fetchedAt,
      };

      const values = columnNames.map((col) => {
        const value = data[col];
        return value === undefined ? null : value;
      });

      const result = insertStmt.run(...values);
      if (result.changes > 0) {
        insertCount++;
      }
    }

    if (insertCount > 0) {
      logger.info("Successfully appended %d new records to database", insertCount);
    } else {
      logger.debug("No new records to append (all duplicates or empty)");
    }
  } catch (e) {
    logger.error("An error occurred during data append: %s", e.message);
  }
}

export function closeDatabase() {
  if (dbInstance) {
    dbInstance.close();
    dbInstance = null;
    logger.info("Database connection closed");
  }
}
