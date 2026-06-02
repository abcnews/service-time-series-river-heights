#!/bin/bash
cd "$(dirname "$0")"

# Load NVM and use correct Node version
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
nvm use || nvm use default

# Run the pipeline
npm run go
