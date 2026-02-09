import ftp from "basic-ftp";
import { Writable } from "node:stream";
import { queue } from "async";
import logger from "./logger.js";

export class MultiClient {
  constructor(pool, config) {
    this.pool = pool;
    this.config = config;
    this.concurrency = config.concurrency || 3;
    
    // Create a queue that processes remote paths using available clients
    this.queue = queue(async (task) => {
      const client = await this.getAvailableClient();
      try {
        return await this.downloadWithClient(client, task.remotePath);
      } finally {
        this.releaseClient(client);
      }
    }, this.concurrency);
  }

  /**
   * Factory method to create and connect a pool of FTP clients.
   */
  static async access(config) {
    const { concurrency = 3, ...ftpConfig } = config;
    const pool = [];

    logger.info(`Initializing FTP pool with ${concurrency} connections to ${ftpConfig.host}...`);

    for (let i = 0; i < concurrency; i++) {
      const client = new ftp.Client();
      // client.ftp.verbose = true; // Enable if debugging is needed
      await client.access(ftpConfig);
      
      if (ftpConfig.cd) {
        await client.cd(ftpConfig.cd);
      }
      
      pool.push({ client, busy: false });
    }

    return new MultiClient(pool, config);
  }

  /**
   * Downloads a file to a buffer and returns it as a string.
   */
  async downloadToBuffer(remotePath) {
    return new Promise((resolve, reject) => {
      this.queue.push({ remotePath }, (err, result) => {
        if (err) return reject(err);
        resolve(result);
      });
    });
  }

  /**
   * Internals: Get an idle client from the pool.
   */
  async getAvailableClient() {
    // Simple wait loop if all busy, but the queue concurrency should prevent this.
    while (true) {
      const entry = this.pool.find(p => !p.busy);
      if (entry) {
        entry.busy = true;
        return entry.client;
      }
      await new Promise(r => setTimeout(r, 100));
    }
  }

  /**
   * Internals: Release a client back to the pool.
   */
  releaseClient(client) {
    const entry = this.pool.find(p => p.client === client);
    if (entry) {
      entry.busy = false;
    }
  }

  /**
   * Internals: Download logic using a specific client instance.
   */
  async downloadWithClient(client, remotePath) {
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

  /**
   * Closes all connections in the pool.
   */
  close() {
    for (const entry of this.pool) {
      entry.client.close();
    }
    logger.info("FTP pool closed.");
  }
}
