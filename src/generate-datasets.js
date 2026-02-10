import { program } from "commander";
import fs from "node:fs/promises";
import path from "node:path";
import { initializeDatabase } from "./sqlite.js";
import { startOfDay, endOfDay, addDays } from "date-fns";
import { toZonedTime, formatInTimeZone } from "date-fns-tz";
import logger from "./logger.js";
import { TABLE_NAME } from "./migrations/00-create-river_data.js";

const TZ = "Australia/Brisbane";

/**
 * Returns UTC boundaries for a given day offset in Brisbane time.
 * @param {number} offset - Day offset (0 for today, -1 for yesterday).
 * @returns {{start: Date, end: Date}}
 */
const getDayBoundaries = (offset = 0) => {
  const brisbaneNow = toZonedTime(new Date(), TZ);
  const target = addDays(brisbaneNow, offset);

  return {
    start: startOfDay(target),
    end: endOfDay(target),
  };
};

/**
 * Loads the station-to-state mapping from gauge-locations.json.
 * @returns {Promise<Map<string, string>>}
 */
async function loadStateMapping() {
  const mappingPath = path.resolve(process.cwd(), "data/gauge-locations.json");
  const content = await fs.readFile(mappingPath, "utf8");
  const data = JSON.parse(content);
  
  const mapping = new Map();
  for (const feature of data.features) {
    const { bom_stn_num, state } = feature.properties;
    if (bom_stn_num && state) {
      mapping.set(String(bom_stn_num), state);
    }
  }
  return mapping;
}

/**
 * Generates datasets for each state for a given day.
 * @param {Object} options - CLI options.
 * @param {string|number} options.dayStart - Day offset (0 for today, -1 for yesterday).
 */
export async function generateDatasets(options) {
  const dayOffset = Number(options.dayStart);
  const { start, end } = getDayBoundaries(dayOffset);
  const dateStr = formatInTimeZone(start, TZ, "yyyy-MM-dd");
  
  logger.info("Generating datasets for %s (AEST)", dateStr);

  const db = initializeDatabase();
  const stateMapping = await loadStateMapping();
  
  // Query for all data in the target day, filtering for Brisbane/AEST boundaries
  const sql = `
    SELECT 
      id, stationName, observedAt, heightM, gaugeDatum, tendency, crossingM, floodClassification
    FROM ${TABLE_NAME}
    WHERE unixepoch(observedAt) BETWEEN ${Math.round(start.getTime() / 1000)} AND ${Math.round(end.getTime() / 1000)}
    ORDER BY unixepoch(observedAt) ASC
  `;
  
  const rows = db.prepare(sql).all();
  
  if (rows.length === 0) {
    logger.info("No data found for %s", dateStr);
    return;
  }

  const stateData = {};

  for (const row of rows) {
    // Determine the state from our gauge-locations metadata mapping
    const state = stateMapping.get(String(row.id)) || "UNKNOWN";
    
    if (!stateData[state]) {
      stateData[state] = {};
    }
    
    if (!stateData[state][row.id]) {
      // Initialize optimized format: arrays of values per field
      stateData[state][row.id] = {
        observedAt: [],
        heightM: [],
        gaugeDatum: [],
        tendency: [],
        crossingM: [],
        floodClassification: []
      };
    }
    
    // Efficiently push values into their respective arrays
    stateData[state][row.id].observedAt.push(row.observedAt);
    stateData[state][row.id].heightM.push(row.heightM);
    stateData[state][row.id].gaugeDatum.push(row.gaugeDatum);
    stateData[state][row.id].tendency.push(row.tendency);
    stateData[state][row.id].crossingM.push(row.crossingM);
    stateData[state][row.id].floodClassification.push(row.floodClassification);
  }

  // Ensure 'UNKNOWN' is handled or logged
  if (stateData.UNKNOWN) {
    const unknownCount = Object.keys(stateData.UNKNOWN).length;
    logger.warn("Found %d stations with unknown state", unknownCount);
    for (const id of Object.keys(stateData.UNKNOWN)) {
      const name = rows.find(r => String(r.id) === id)?.stationName;
      logger.warn("  - Station %s: %s", id, name);
    }
  }

  // Output to data/assets/[state]/[date].json
  for (const [state, stations] of Object.entries(stateData)) {
    const dir = path.resolve(process.cwd(), "data/assets", state.toLowerCase());
    await fs.mkdir(dir, { recursive: true });
    
    const filePath = path.join(dir, `${dateStr}.json`);
    const output = {
      updatedAt: formatInTimeZone(new Date(), TZ, "yyyy-MM-dd'T'HH:mm:ssXXX"),
      date: dateStr,
      stations
    };
    
    await fs.writeFile(filePath, JSON.stringify(output), "utf8");
    logger.info("Wrote %s dataset for %s to %s", state, dateStr, filePath);
  }
}

export default generateDatasets;


// CLI setup
if (import.meta.url === `file://${process.argv[1]}`) {
  program
    .option("-d, --dayStart <number>", "Day relative to today (0, -1, -2, etc.)", "0")
    .parse();

  const options = program.opts();
  await generateDatasets(options).catch(err => {
    logger.error("Failed to generate datasets: %s", err.message);
    process.exit(1);
  });
}
