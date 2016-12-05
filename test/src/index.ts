import { Database, TransactionOptions } from '../../';
import temp = require('temp');
import { test } from 'ava';
import * as path from 'path';
import * as fse from 'fs-extra-promise';
import _ = require('highland');
temp.track();

async function generateArthurDatabase(): Promise<Database> {
  const db = new Database(':memory:');

  const t = await db.beginTransaction();

  await t.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await t.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Bart", "Simpson");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Smith");
  await t.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Lowe");
  await t.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});

  await t.commit();

  return db;
}

const transactionOptions:(TransactionOptions | undefined)[] = [{type: "IMMEDIATE"}, {type: "DEFERRED"}, {type: "EXCLUSIVE"}, undefined];

test('transactions', async (t) => {
  for (let typeOpts of transactionOptions) {

    const db = await generateArthurDatabase();
    const transaction = await db.beginTransaction(typeOpts);

    await transaction.runAsync(`UPDATE People SET firstname = 'Jef' WHERE lastname = 'Smith'`);

    const p = db.runAsync(`UPDATE People SET firstname = 'Geoff' WHERE lastname = 'Smith'`);

    const shouldBeJef = await transaction.allAsync(`SELECT * from People WHERE lastname = 'Smith'`);
    t.is(shouldBeJef[0].firstname, 'Jef');

    await transaction.commit();
    await p;

    const shouldBeGeoff = await db.allAsync(`SELECT * from People WHERE lastname = 'Smith'`);
    t.is(shouldBeGeoff[0].firstname, 'Geoff');
  }
});

test('nested transactions', async (t) => {
  for (let typeOpts of transactionOptions) {
    const db = await generateArthurDatabase();
    const t1 = await db.beginTransaction(typeOpts);

    await t1.runAsync(`UPDATE People SET firstname = 'Jef' WHERE lastname = 'Smith'`);
    const shouldBeJef = await t1.allAsync(`SELECT * from People WHERE lastname = 'Smith'`);
    t.is(shouldBeJef[0].firstname, 'Jef');

    const t2 = await t1.beginTransaction();

    await t2.runAsync(`UPDATE People SET firstname = 'Geoff' WHERE lastname = 'Smith'`);

    //execute this in background for when t1 is finished
    t1.runAsync(`UPDATE People SET firstname = 'Jeff' WHERE lastname = 'Smith'`);

    const shouldBeGeoff = await t2.allAsync(`SELECT * from People WHERE lastname = 'Smith'`);
    t.is(shouldBeGeoff[0].firstname, 'Geoff');

    await t2.commit();
    await t1.commit();

    const shouldBeJeff = await t1.allAsync(`SELECT * from People WHERE lastname = 'Smith'`);
    t.is(shouldBeJeff[0].firstname, 'Jeff');
  }
});


test('basic open and read/write', async (t) => {
  const db = await generateArthurDatabase();
  const rows: any[] = await db.allAsync('SELECT * from People;');
  t.plan(7);
  t.is(rows.length, 6, "Six rows");

  await db.select('SELECT * from People').do(row => {
    t.truthy(true);
  }).toPromise();
});

/*test('stream', async (t) => {*/
  //const db = await generateArthurDatabase();
  //const rows = db.eachStream('SELECT * from People where firstname is ?;', "Arthur");

  //t.plan(4);
  //await new Promise( (resolve, reject) => {
    //rows.on('data', (row: any) => {
      //t.is(row.firstname, "Arthur", "Should get an Arthur");
    //});

    //rows.on('error', (err: Error) => {
      //reject(err);
    //});

    //rows.on('end', () => {
      //t.truthy(true, "Finished");
      //resolve();
    //});
  //});
//});

/*test('highland', async (t) => {*/
  //const db = await generateArthurDatabase();
  //const rows = db.eachStream('SELECT * from People where firstname is ?;', "Arthur");
  //t.plan(3);
  //await new Promise( (resolve, reject) => {
    //_(rows).map((row: any) => {
      //t.is(row.firstname, "Arthur", "Should get an Arthur");
      //return row;
    //}).toArray(resolve);
  //});
//});

test('eachAsync', async (t) => {
  const db = await generateArthurDatabase();
  const numRows = 6;

  t.plan(numRows * 2 + 1);

  const names = ["Jeff", "Bart", "Arthur", "Arthur", "Arthur", "Bender"];

  const num = await db.eachAsync('SELECT * from People;', (err:Error, row:any) => {
    t.is(err, null, "No error");
    t.is(row.firstname, names.shift(), "Row exists");
  });

  t.is(num, numRows, "Six rows");
});

test('errors', async (t) => {
  const db = new Database(':memory:');
  t.plan(2);

  try {
    await db.runAsync('INSERT INTO NoTable VALUES ("Jeff", "Smith");');
  } catch (error) {
    t.truthy(true);
  }

  t.throws(db.runAsync('INSERT INTO NoTable VALUES ("Jeff", "Smith");'));
});

test('preparedStatements', async (t) => {
  const db = new Database(':memory:');

  await db.execAsync('CREATE TABLE People (firstname TEXT, lastname TEXT);');
  await db.runAsync('INSERT INTO People VALUES ("Jeff", "Smith");');
  await db.runAsync('INSERT INTO People VALUES (?, ?);', ["Bart", "Simpson"]);
  await db.runAsync('INSERT INTO People VALUES (?, ?);', "Arthur", "Dent");
  await db.runAsync('INSERT INTO People VALUES ($firstname, $lastname);', {$firstname: "Bender", $lastname:"Rodríguez"});

  t.plan(2 + 6 + 1 + 1);

  const names = ["Jeff", "Bart", "Arthur", "Bender"];

  const statement = await db.prepareAsync('SELECT * from People WHERE lastname = ?');

  let num = await db.eachAsync(statement, (err: Error, row: any) => {
    t.is(err, null, "No error");
    t.is(row.lastname, 'Rodríguez', "Row exists");
  }, 'Rodríguez');
  t.is(num, 1, "Retrieved one row");

  const insertStatement = await db.prepareAsync('INSERT INTO People VALUES (?, ?)');
  await db.runAsync(insertStatement, 'Adrián', 'Rodríguez');
  await insertStatement.bindAsync('Simón', 'Rodríguez');
  await db.runAsync(insertStatement);

  num = await db.eachAsync(statement, (err: Error, row: any) => {
    t.is(err, null, "No error");
    t.is(row.lastname, 'Rodríguez', "Row exists");
  }, 'Rodríguez');

  t.is(num, 3, "Retrieved three rows");

});

test('select() completes with zero results', async t => {
  const db = await generateArthurDatabase();
  let results = await db.select('SELECT * from People WHERE lastname = "Incognito"').toArray().toPromise();
  t.is(results.length, 0, "Should finish with 0 results");
  results = await db.select('SELECT * from People WHERE lastname = "Incognito"').toArray().toPromise();
  t.is(results.length, 0, "Should finish with 0 results");
});

test('select() completes with bogus query', async t => {
  const db = await generateArthurDatabase();
  const tr = await db.beginTransaction();
  t.throws(tr.select('SELECT * from People WHERE json_extract(lastname, "$.last") = "Incognito"').toArray().toPromise());
  t.throws(tr.select('SELECT * from People WHERE json_extract(lastname, "$.last") = "Incognito"').toArray().toPromise());
  t.throws(tr.select('NONSENSE QUERY').toArray().toPromise());
});
