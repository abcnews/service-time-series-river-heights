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

/**
 * Prunes entries from the river_data table that are older than the specified number of days.
 * @param {number} [days=process.env.DB_PRUNE_DAYS || 7] - The age in days beyond which records should be deleted.
 * @returns {number} - The number of rows deleted.
 */
export function pruneEntries(days = process.env.DB_PRUNE_DAYS || 7) {
  const db = initializeDatabase();

  try {
    logger.info("Pruning entries older than %d days...", days);

    // Explicitly convert days to a negative string for SQLite's datetime modifier
    const daysModifier = `-${days} days`;

    // Use datetime with UTC to ensure consistency with toISOString()
    const deleteStmt = db.prepare(
      `DELETE FROM ${TABLE_NAME} WHERE datetime(observedAt) < datetime('now', ?)`,
    );

    const result = deleteStmt.run(daysModifier);

    if (result.changes > 0) {
      logger.info(
        "Successfully pruned %d entries",
        result.changes,
      );

      // Vacuuming reclaims disk space after a large deletion
      logger.info("Vacuuming database to reclaim space (this may take a few seconds)...");
      db.exec("VACUUM");
      logger.info("Vacuum complete");
    } else {
      logger.info("No entries older than %d days found to prune", days);
    }

    return result.changes;
  } catch (e) {
    logger.error("An error occurred during data pruning: %s", e.message);
    return 0;
  }
}

export function closeDatabase() {
  if (dbInstance) {
    dbInstance.close();
    dbInstance = null;
    logger.info("Database connection closed");
  }
}
