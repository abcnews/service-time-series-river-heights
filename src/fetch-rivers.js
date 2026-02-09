import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseRiverHeights } from "./parse-rivers.js";
import logger from './logger.js'
import { appendRecords } from "./sqlite.js";
import { MultiClient } from "./ftp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export async function fetchRequestedProducts() {
  const productsPath = path.join(__dirname, "bom-products.json");
  const products = JSON.parse(await fs.readFile(productsPath, "utf8"));

  const allRecords = [];

  const multiClient = await MultiClient.access({
    host: "ftp.bom.gov.au",
    user: "anonymous",
    password: process.env.BOM_FTP_PASSWORD || "guest",
    secure: false,
    cd: "anon/gen/fwo",
    concurrency: 3
  });

  try {
    // Process products in parallel using the MultiClient pool
    await Promise.all(products.map(async (product) => {
      if (!product.filename) return;

      try {
        const html = await multiClient.downloadToBuffer(product.filename);
        logger.info(`Fetched ${product.filename}...`);
        const result = await parseRiverHeights(html);
        allRecords.push(...result.records);
      } catch (err) {
        logger.error(`Failed to process ${product.filename}: %s`, err.message);
      }
    }));

    if (allRecords.length > 0) {
      // Append to SQLite database
      await appendRecords(allRecords);
    } else {
      logger.info("No records found to process.");
    }
  } catch (err) {
    logger.error("FTP Error: %O", err);
  } finally {
    multiClient.close();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  fetchRequestedProducts();
}
