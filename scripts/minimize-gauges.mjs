import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

/**
 * Minimizes the gauge locations GeoJSON by stripping unnecessary properties,
 * removing non-standard IDs, sorting by basin, and minifying the output.
 */
const minimizeGauges = async () => {
	const __dirname = import.meta.dirname;
	const inputPath = resolve(__dirname, '../data/gauge-locations.json');
	const outputPath = resolve(__dirname, '../data/gauge-locations-minimal.json');

	console.log(`Reading gauge data from ${inputPath}...`);

	const data = await readFile(inputPath, 'utf-8').catch((error) => {
		console.error('Failed to read gauge locations:', error);
		process.exit(1);
	});

	const geojson = JSON.parse(data);

	// Transform features to keep only required properties and remove id
	const minimizedFeatures = geojson.features.map(({ type, geometry, properties }) => ({
		type,
		geometry,
		properties: {
			id: properties.bom_stn_num,
			name: properties.name,
			basin: properties.basin,
		},
	}));

	// Sort features by basin name
	const sortedFeatures = minimizedFeatures.sort((a, b) => {
		const basinA = a.properties.basin || '';
		const basinB = b.properties.basin || '';
		return basinA.localeCompare(basinB);
	});

	const minimizedGeojson = {
		...geojson,
		features: sortedFeatures,
	};

	console.log(`Writing minimized data to ${outputPath}...`);

	// Write without pretty-printing (no indentation)
	await writeFile(outputPath, JSON.stringify(minimizedGeojson), 'utf-8').catch((error) => {
		console.error('Failed to write minimized gauge locations:', error);
		process.exit(1);
	});

	console.log('Successfully minimized and sorted gauge locations.');
};

minimizeGauges();
