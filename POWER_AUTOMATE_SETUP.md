# Power Automate Setup

This is the recommended browser-only workflow for the `GivingData Sync Files` SharePoint folder.

## Goal

Users should be able to:

1. upload the latest Organizations, Requests, and Payments CSV exports into the SharePoint folder
2. click `Sync` from SharePoint
3. have the newest three files processed automatically
4. upsert the data into Airtable without breaking Org -> Request -> Payment relationships

## Recommended Architecture

Use:

- SharePoint document library button
- Power Automate instant flow
- HTTP action from Power Automate
- a small hosted sync endpoint that runs the importer in this repo

Power Automate should orchestrate the files.
The hosted sync endpoint should perform the actual Airtable upsert logic because the importer already handles:

- newest file selection logic
- CSV cleanup
- safe Airtable upserts
- record-link preservation using Airtable record IDs

## What To Build In Power Automate

### 1. Create the flow

In the SharePoint document library:

1. Open the `GivingData Sync Files` folder.
2. In the command bar, select `Automate`.
3. Select `Power Automate`.
4. Select `Create a flow`.
5. Choose `Show more`.
6. Choose `Complete a custom action for the selected item`.

This creates a SharePoint flow that appears back in the `Automate` menu for the library.

Name it:

`GivingData Sync`

## 2. Trigger

Use the SharePoint trigger:

`For a selected file`

Even though the end-user experience is “sync this folder”, the selected-file trigger is the SharePoint-native way to expose a flow in the library UI.
The user can select any file in the folder and click `Automate -> GivingData Sync`.
The flow should ignore the selected file and instead process the newest matching files in the folder.

## 3. Flow steps

Build the flow in this order.

### Step A. Initialize variables

Add these variables:

- `siteAddress`
- `libraryName`
- `targetFolderPath`
- `organizationsFileId`
- `requestsFileId`
- `paymentsFileId`

Set:

- `siteAddress` = your SharePoint site
- `libraryName` = your library
- `targetFolderPath` = the server-relative folder path for `GivingData Sync Files`

Example:

`/sites/Tech/GivingData Sync/GivingData Sync Files`

### Step B. List files in the target folder

Action:

`Get files (properties only)` from SharePoint

Set:

- `Site Address` = your site
- `Library Name` = your library
- `Limit entries to folder` = `targetFolderPath`

This gives you all files currently in the folder.

### Step C. Filter to CSVs only

Action:

`Filter array`

Condition:

- filename ends with `.csv`

### Step D. Identify newest file for each type

Create three filtered arrays:

- Organizations: filename contains `organization`
- Requests: filename contains `request`
- Payments: filename contains `payment`

For each of those arrays:

1. Sort descending by `Modified`
2. Take the first item

Store the file identifiers in:

- `organizationsFileId`
- `requestsFileId`
- `paymentsFileId`

Expected filename patterns:

- `*organization*.csv`
- `*request*.csv`
- `*payment*.csv`

### Step E. Fail fast if any file is missing

Add a `Condition` that checks all three file IDs are present.

If any are missing:

- terminate the flow as failed
- include an error like:

`Could not find newest Organizations, Requests, and Payments CSVs in GivingData Sync Files`

### Step F. Read file contents

Add three SharePoint actions:

`Get file content`

Use the file identifiers from the previous step.

### Step G. Send one HTTP request to the sync service

Action:

`HTTP`

Method:

`POST`

URL:

your hosted endpoint, for example:

`https://your-sync-service.example.com/sync`

Headers:

- `Content-Type: application/json`
- optional shared secret header if you secure the endpoint

Body:

```json
{
  "organizations_filename": "@{outputs('Get_file_properties_org')?['body/{FilenameWithExtension}']}",
  "organizations_content_base64": "@{body('Get_file_content_org')?['$content']}",
  "requests_filename": "@{outputs('Get_file_properties_requests')?['body/{FilenameWithExtension}']}",
  "requests_content_base64": "@{body('Get_file_content_requests')?['$content']}",
  "payments_filename": "@{outputs('Get_file_properties_payments')?['body/{FilenameWithExtension}']}",
  "payments_content_base64": "@{body('Get_file_content_payments')?['$content']}"
}
```

The sync service should:

- decode the three files
- run the same normalization/import logic from this repo
- upsert Organizations first
- resolve Airtable Org record IDs
- upsert Requests
- resolve Airtable Request record IDs
- upsert Payments

### Step H. Show success/failure

Add a final `Condition` on the HTTP response status.

If success:

- return a friendly message:

`GivingData sync completed successfully`

If failure:

- return the error body from the service

## 4. Button behavior in SharePoint

Once saved, the flow should appear in the SharePoint library under:

`Automate -> GivingData Sync`

That is the browser button users click.

## 5. Important implementation notes

- Do not let Power Automate write directly to Airtable links by raw text if you want relationships preserved.
- The importer in this repo is already hardened to resolve Airtable record links safely.
- Keep the relationship logic in the sync service, not in Power Automate expressions.
- If a referenced Organization or Request does not exist yet, the importer should leave the link unset and report it, rather than guessing.

## 6. What still needs to be built

Power Automate alone is not enough for the safest version.

You still need a small hosted sync service around this repo so the flow can call it over HTTP.

That service should wrap:

- `seed_airtable_from_csv.py`
- `sync_givingdata_airtable.py`

and expose one endpoint:

`POST /sync`

with the JSON payload described above.
