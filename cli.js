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
      "./dataBomRiver/fetch-rivers.js"
    );
    await fetchRequestedProducts();
  })
  .command("upload-s3")
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
