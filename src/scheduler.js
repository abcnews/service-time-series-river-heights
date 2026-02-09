import cron from 'node-cron';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import logger from './logger.js';

const execAsync = promisify(exec);

/**
 * Runs the data processing pipeline
 */
async function runPipeline() {
  logger.info('🚀 Starting scheduled pipeline run...');
  const startTime = Date.now();

  try {
    // Run the 'go' script from package.json
    const { stdout, stderr } = await execAsync('npm run go');
    
    if (stdout) logger.info(`STDOUT: ${stdout}`);
    if (stderr) logger.warn(`STDERR: ${stderr}`);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info(`✨ Pipeline run completed successfully in ${duration}s.`);
  } catch (error) {
    logger.error(`🛑 Pipeline run failed: ${error.message}`);
    if (error.stdout) logger.error(`STDOUT: ${error.stdout}`);
    if (error.stderr) logger.error(`STDERR: ${error.stderr}`);
  }

  logger.info('Next run scheduled for 20 minutes from now.');
}

// Schedule the task to run every 20 minutes
// Pattern: minute hour day-of-month month day-of-week
cron.schedule('*/20 * * * *', () => {
  runPipeline();
});

// Run once immediately on startup
logger.info('Scheduler started. Running initial pipeline...');
runPipeline();
