import { Database, TransactionOptions } from '../../../';
import { test } from 'ava';
import * as path from 'path';
import { generateArthurDatabase } from '../common/index';


test('Don\'t deadlock on await', async t => {
  const db = await generateArthurDatabase();
  const tr = await db.beginTransaction();
  setTimeout(() => {
    tr.runAsync("INSERT INTO People VALUES ('Fred', 'Flintstone')").then(() => {
      tr.commit()
    });
  }, 300);
  const people = await db.allAsync('SELECT * FROM People');
  t.is(people.length, 7, "Completed in order, without deadlocking");
});

