import { SQLite, Database } from '../../';
import temp = require('temp');
import { test } from 'ava';
import * as path from 'path';
import * as fse from 'fs-extra-promise';
import _ = require('highland');
temp.track();

async function generateArthurDatabase(): Promise<Database> {
  const db = await SQLite.open(':memory:');

  await db.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await db.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await db.runAsync('INSERT INTO People VALUES (?, ?);', ["Bart", "Simpson"]);
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Smith");
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Lowe");
  await db.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});

  return db;
}

test('basic open and read/write', async (t) => {
  const db = await generateArthurDatabase();
  try {
    const rows = await db.allAsync('SELECT * from People;');
    t.is(rows.length, 6, "Six rows");
  } catch (error) {
    console.log(error);
    t.fail(error);
  }
});

test('stream', async (t) => {
  const db = await generateArthurDatabase();
  const rows = db.eachStream('SELECT * from People where firstname is ?;', "Arthur");

  t.plan(4);
  await new Promise( (resolve, reject) => {
    rows.on('data', (row: any) => {
      t.is(row.firstname, "Arthur", "Should get an Arthur");
    });

    rows.on('error', (err: Error) => {
      reject(err);
    });

    rows.on('end', () => {
      t.truthy(true, "Finished");
      resolve();
    });
  });
});

test('highland', async (t) => {
  const db = await generateArthurDatabase();
  const rows = db.eachStream('SELECT * from People where firstname is ?;', "Arthur");
  t.plan(3);
  await new Promise( (resolve, reject) => {
    _(rows).map((row: any) => {
      t.is(row.firstname, "Arthur", "Should get an Arthur");
      return row;
    }).toArray(resolve);
  });
});

test('eachAsync', async (t) => {
  const db = await generateArthurDatabase();
  const numRows = 6;

  try {
    t.plan(numRows * 2 + 1);

    const names = ["Jeff", "Bart", "Arthur", "Arthur", "Arthur", "Bender"];

    const num = await db.eachAsync('SELECT * from People;', (err, row) => {
      t.is(err, null, "No error");
      t.is(row.firstname, names.shift(), "Row exists");
    });

    t.is(num, numRows, "Three rows");
  } catch (error) {
    console.log(error);
    t.fail(error);
  }
});

test('preparedStatements', async (t) => {
  const db = await SQLite.open(':memory:');

  await db.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await db.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await db.runAsync('INSERT INTO People VALUES (?, ?);', ["Bart", "Simpson"]);
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await db.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});

  try {
    t.plan(3);

    const names = ["Jeff", "Bart", "Arthur", "Bender"];

    const statement = await db.prepareAsync('SELECT * from People WHERE lastname = ?');

    const num = await statement.eachAsync('Rodríguez', (err, row) => {
      t.is(err, null, "No error");
      t.is(row.lastname, 'Rodríguez', "Row exists");
    });

    t.is(num, 1, "Retrieved one row");
  } catch (error) {
    console.log(error);
    t.fail(error);
  }
});

