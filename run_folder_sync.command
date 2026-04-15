#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f ".env" ]]; then
  set -a
  source ".env"
  set +a
fi

if [[ -z "${GD_SYNC_FOLDER:-}" ]]; then
  echo "GD_SYNC_FOLDER is not set."
  echo "Add it to .env and point it at your synced SharePoint folder."
  echo "Example:"
  echo "GD_SYNC_FOLDER=\"/Users/yourname/Library/CloudStorage/OneDrive-YourOrg/Tech/GivingData Sync/GivingData Sync Files\""
  read -k 1 "?Press any key to close..."
  exit 1
fi

echo "Syncing latest CSVs from:"
echo "$GD_SYNC_FOLDER"
echo

python3 seed_airtable_from_csv.py --folder "$GD_SYNC_FOLDER"

echo
echo "Sync complete."
read -k 1 "?Press any key to close..."
