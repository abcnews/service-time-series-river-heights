# service-time-series-river-heights

Fetch and parse BOM river gauge heights and store them in a local SQLite database.

The scripts are wrapped in Commander, so you can run `node . --help` for details.


## Setup

1. **Install Dependencies**:
   ```bash
   npm install
   ```

2. **Environment Configuration**:
   Create a `.env` file in the root directory.

## Usage

### Fetch Data

To fetch the latest river heights and append them to the database:

```bash
npm run fetch-rivers
```

This will:
1. Connect to the BOM FTP server.
2. Download and parse the products defined in `dataBomRiver/bom-products.json`.
3. Append new records to `data/rivers.sqlite`.

## Development

### Running Tests

```bash
npm test
```
