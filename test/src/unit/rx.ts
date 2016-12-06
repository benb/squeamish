import { Database, TransactionOptions } from '../../../';
import temp = require('temp');
import { test } from 'ava';
import * as path from 'path';
import * as fse from 'fs-extra-promise';
import { generateArthurDatabase } from '../common/index';
temp.track();

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
