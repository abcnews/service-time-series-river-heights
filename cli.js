#!/usr/bin/env node
import { program } from "commander";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

program
  .command("fetch-rivers")
  .description("Fetch river height data from BOM FTP")
  .action(async () => {
    const { fetchRequestedProducts } = await import(
      "./src/fetch-rivers.js"
    );
    await fetchRequestedProducts();
  });

program
  .command("generate-datasets")
  .description("Generate daily datasets for river heights optimized for the visualization")
  .option("-d, --dayStart <number>", "Day relative to today (0, -1, -2, etc.)", "0")
  .option("-n, --days <number>", "Number of days to generate", process.env.GENERATE_DAYS || "2")
  .action(async (options) => {
    const { generateDatasets } = await import("./src/generate-datasets.js");
    const count = Number(options.days);
    const start = Number(options.dayStart);
    for (let i = 0; i < count; i++) {
      await generateDatasets({ dayStart: start - i });
    }
  });

program
  .command("prune")
  .description("Prune entries from the database older than 7 days")
  .option(
    "-d, --days <days>",
    "Number of days to keep",
    process.env.DB_PRUNE_DAYS || "7",
  )
  .action(async (options) => {
    const { pruneEntries, closeDatabase } = await import("./src/sqlite.js");
    await pruneEntries(options.days);
    closeDatabase();
  });

program
  .command("get-db")
  .description("Download and unzip the river database from S3")
  .action(async () => {
    const { S3_BUCKET, S3_END_POINT, S3_DEST } = process.env;
    const url = `https://${S3_BUCKET}.${S3_END_POINT}/${S3_DEST}rivers.sqlite.gz`;
    console.log(`Downloading database from ${url}...`);

    const { exec } = await import("node:child_process");
    const { promisify } = await import("node:util");
    const fs = await import("node:fs/promises");

    // Ensure the data directory exists
    await fs.mkdir("data", { recursive: true });

    const execAsync = promisify(exec);
    await execAsync(`curl -s ${url} | gunzip > data/rivers.sqlite`);
  });

program.command("upload-s3")
  .description("Upload files to S3")
  .option("-e, --end-point <server>", "S3 endpoint", process.env.S3_END_POINT)
  .option("-p, --port <port>", "Port", process.env.S3_PORT || 443)
  .option(
    "-a, --access-key <accessKey>",
    "s3 access key",
    process.env.S3_ACCESS_KEY,
  )
  .option(
    "-k, --secret-key <secretKey>",
    "s3 secret key",
    process.env.S3_SECRET_KEY,
  )
  .option("-b --bucket <bucket>", "Bucket", process.env.S3_BUCKET)
  .option(
    "-s, --src <srcDir>",
    "source directory",
    process.env.S3_SRC || "data/",
  )
  .option("-d, --dest <destDir>", "destination directory", process.env.S3_DEST)
  .action(async (options) => {
    const { default: uploadS3 } = await import("./src/upload-s3.mjs");
    await uploadS3(options);
  });

program.parse();
