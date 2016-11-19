import { SQLite } from '../../';
import temp = require('temp');
import { test } from 'ava';
import * as path from 'path';
import * as fse from 'fs-extra-promise';
temp.track();

test('basic open and read/write', async (t) => {
  const db = await SQLite.open(':memory:');

  await db.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await db.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  try {
    const rows = await db.allAsync('SELECT * from People;');
    t.is(rows.length, 1, "One row");
  } catch (error) {
    console.log(error);
    t.fail(error);
  }
});

test('eachAsync', async (t) => {
  const db = await SQLite.open(':memory:');

  await db.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await db.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await db.runAsync('INSERT INTO People VALUES (?, ?);', ["Bart", "Simpson"]);
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await db.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});
  const numRows = 4;

  try {
    t.plan(numRows * 2 + 1);

    const names = ["Jeff", "Bart", "Arthur", "Bender"];

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

