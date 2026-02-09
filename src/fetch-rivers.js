import ftp from "basic-ftp";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Writable } from "node:stream";
import { parseRiverHeights } from "./parse-rivers.js";
import logger from './logger.js'
import { appendRecords } from "./sqlite.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Helper to download a file to a buffer using basic-ftp
 */
async function downloadToBuffer(client, remotePath) {
  let buffer = Buffer.alloc(0);
  const writable = new Writable({
    write(chunk, encoding, callback) {
      buffer = Buffer.concat([buffer, chunk]);
      callback();
    },
  });

  await client.downloadTo(writable, remotePath);
  return buffer.toString("utf8");
}

export async function fetchRequestedProducts() {
  const productsPath = path.join(__dirname, "bom-products.json");
  const products = JSON.parse(await fs.readFile(productsPath, "utf8"));

  const client = new ftp.Client();
  client.ftp.verbose = true;
  const allRecords = [];

  try {
    const accessConfig = {
      host: "ftp.bom.gov.au",
      user: "anonymous",
      password: process.env.BOM_FTP_PASSWORD || "guest",
      secure: false,
    };

    logger.info(`Connecting to BOM FTP at ${accessConfig.host} as ${accessConfig.user}...`);
    await client.access(accessConfig);

    logger.info("Connected to BOM FTP. Navigating to /anon/gen/fwo/...");
    await client.cd("anon/gen/fwo");

    for (const product of products) {
      if (!product.filename) {
        continue;
      }

      logger.info(`Fetching and parsing ${product.filename}...`);
      try {
        const html = await downloadToBuffer(client, product.filename);
        const result = await parseRiverHeights(html);
        allRecords.push(...result.records);
      } catch (err) {
        logger.error(`Failed to process ${product.filename}: %s`, err.message);
      }
    }

    if (allRecords.length > 0) {
      // Append to SQLite database
      await appendRecords(allRecords);
    } else {
      logger.info("No records found to process.");
    }
  } catch (err) {
    logger.error("FTP Error: %O", err);
  } finally {
    client.close();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  fetchRequestedProducts();
}
