import { Database, TransactionOptions } from '../../../';
import { test } from 'ava';
import * as  temp from 'temp';
import * as path from 'path';
import { generateArthurDatabase } from '../common/index';

temp.track();

test('Handle SQLITE_BUSY', async t => {
  const filename = path.join(temp.mkdirSync(), "db");
  const db = await generateArthurDatabase(filename);
  const db2 = new Database(filename);
  const initialPeople = await db.allAsync('SELECT * FROM People');
  const tr = await db.beginTransaction({type: 'EXCLUSIVE'});
  const p1 = tr.runAsync("INSERT INTO People VALUES ('Fred', 'Flintstone')");
  const p2 = db2.beginTransaction({type: 'EXCLUSIVE'}).then( tr2 => {
    return tr2.runAsync("INSERT INTO People VALUES ('Wilma', 'Flintstone')")
              .then(() => tr2.commit());
  });
  setTimeout(() => {
    tr.commit();
  }, 200);
  await p1;
  await p2;
  const people = await db.allAsync('SELECT * FROM People');
  t.is(people.length, initialPeople.length + 2, "Should have added 2 people");
});
