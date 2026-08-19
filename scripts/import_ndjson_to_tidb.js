import fs from 'fs';
import path from 'path';
import readline from 'readline';
import mysql from 'mysql2/promise';

const args = process.argv.slice(2);
if (args.length < 2) {
    console.error("Usage: node import_ndjson_to_tidb.js <YYYY-MM-DD> <path_to_extracted_ndjson_folder>");
    process.exit(1);
}

const releaseDate = args[0];
const sourceFolder = args[1];

// We can read tidbUri from args or env. Since process.env might not be populated in this script if it's run standalone without a .env loader, we'll construct it or rely on it.
const host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com";
const port = 4000;
const user = "ktQaY5Y9x7TFRvV.root";
const password = "qUlM80c5MitEvh2e";
const database = "test"; 

async function main() {
    let connection;
    try {
        connection = await mysql.createConnection({
            host, port, user, password, database,
            ssl: { rejectUnauthorized: true }
        });
        console.log(`Connected to TiDB. Ready to import data for release ${releaseDate}.`);

        const files = fs.readdirSync(sourceFolder).filter(f => f.endsWith('.ndjson'));
        if (files.length === 0) {
            console.log("No .ndjson files found in the specified directory.");
            return;
        }

        let totalInserted = 0;
        
        for (const file of files) {
            const filePath = path.join(sourceFolder, file);
            console.log(`Processing ${file}...`);
            
            const fileStream = fs.createReadStream(filePath);
            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity
            });

            let batch = [];
            const batchSize = 1000;
            
            for await (const line of rl) {
                if (!line.trim()) continue;
                
                try {
                    const obj = JSON.parse(line);
                    // For new voters, default the first release to No, and set the current release to Yes
                    if (releaseDate !== '2026-08-03') {
                         obj['ASDDO_03to10_Aug'] = 'No';
                    }
                    const releaseFieldName = `ASDDO_${releaseDate.replace(/-/g, '_')}`;
                    obj[releaseFieldName] = 'Yes';
                    
                    batch.push([JSON.stringify(obj)]);
                } catch(e) {
                    console.error("Invalid JSON on line:", line);
                }

                if (batch.length >= batchSize) {
                    await insertBatch(connection, batch, releaseDate);
                    totalInserted += batch.length;
                    batch = [];
                }
            }

            if (batch.length > 0) {
                await insertBatch(connection, batch, releaseDate);
                totalInserted += batch.length;
            }
        }
        
        console.log(`\nImport complete! Total records inserted/updated: ${totalInserted}`);

    } catch (error) {
        console.error("Error during import:", error);
    } finally {
        if (connection) await connection.end();
    }
}

async function insertBatch(connection, batch, releaseDate) {
    const releaseFieldName = `ASDDO_${releaseDate.replace(/-/g, '_')}`;
    const query = `
        INSERT INTO asddo_voters (data) VALUES ?
        ON DUPLICATE KEY UPDATE 
        data = JSON_SET(data, '$.${releaseFieldName}', 'Yes')
    `;
    await connection.query(query, [batch]);
}

main();
